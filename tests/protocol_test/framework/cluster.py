"""High-level SiMM cluster lifecycle management for test fixtures.

Supports both single-machine and multi-machine deployments.
In multi-machine mode, the test runner (node A) orchestrates CM/DS processes
on remote hosts via SSH.
"""

import logging
import os
import time
from pathlib import Path

from .admin_client import AdminClient
from .cluster_observer import ClusterObserver
from .config import ClusterConfig
from .fault_injector import FaultInjector
from .log_parser import LogParser
from .port_allocator import PortAllocator
from .process_manager import ProcessHandle, ProcessManager
from .ssh_executor import SshConfig, SshExecutor

logger = logging.getLogger(__name__)


class SimmCluster:
    """
    High-level cluster object providing full lifecycle management.
    Used as a pytest fixture.

    Supports two modes:
    - Single-machine: all processes on localhost (config.cm_host is None)
    - Multi-machine: CM and DS on specified remote hosts via SSH
    """

    def __init__(self, config: ClusterConfig, log_dir: str | Path | None = None,
                 build_dir: str | Path | None = None):
        self.config = config

        # Initialize SSH executor
        ssh_config = SshConfig(user=config.ssh_user, port=config.ssh_port)
        self._ssh = SshExecutor(ssh_config=ssh_config)
        self._is_multi_machine = config.cm_host is not None

        # Determine default build_dir and log_dir
        if build_dir is not None:
            default_build_dir = str(build_dir)
        elif os.environ.get("SIMM_BUILD_DIR"):
            default_build_dir = os.environ["SIMM_BUILD_DIR"]
        else:
            simm_root = Path(__file__).parents[3]
            default_build_dir = str(simm_root / "build" / config.build_mode / "bin")

        if log_dir is not None:
            default_log_dir = str(log_dir)
        else:
            default_log_dir = "/tmp/simm_test_logs"

        self._default_build_dir = default_build_dir
        self._default_log_dir = default_log_dir

        # Validate binaries exist (check on local for single-machine,
        # or on remote hosts for multi-machine)
        if not self._is_multi_machine:
            for binary in ["cluster_manager", "data_server"]:
                p = Path(default_build_dir) / binary
                if not p.exists():
                    raise FileNotFoundError(
                        f"Binary '{binary}' not found at {default_build_dir}. "
                        f"Build with: ./build.sh --mode={config.build_mode}"
                    )

        self._port_allocator = PortAllocator(ssh_executor=self._ssh)
        self._process_manager = ProcessManager(
            default_build_dir, default_log_dir,
            self._port_allocator, self._ssh,
        )

        # Detect RDMA availability
        self._use_rpc = not os.environ.get("SIMM_TEST_NO_RDMA")

        # Admin client — runs locally, connects to remote nodes via RPC
        ctl_bin = Path(default_build_dir) / "simm_ctl_admin"
        flags_bin = Path(default_build_dir) / "simm_flags_admin"
        self._admin_client = AdminClient(ctl_bin, flags_bin)

        # State
        self.cm: ProcessHandle | None = None
        self.data_servers: list[ProcessHandle] = []

        # Initialized after start()
        self.observer: ClusterObserver | None = None
        self.fault_injector: FaultInjector | None = None

    def _verify_ssh_connectivity(self) -> None:
        """Verify SSH connectivity to all remote hosts."""
        hosts_to_check = set()
        if self.config.cm_host:
            hosts_to_check.add(self.config.cm_host.ip)
        for dh in self.config.ds_hosts:
            hosts_to_check.add(dh.ip)

        for host in hosts_to_check:
            if not self._ssh.is_local(host):
                if not self._ssh.check_connectivity(host):
                    raise RuntimeError(
                        f"SSH connectivity check failed for {host}. "
                        f"Ensure passwordless SSH is configured."
                    )
        logger.info("SSH connectivity verified for %d hosts", len(hosts_to_check))

    def start(self) -> None:
        """Start CM, then start all DS (on local or remote hosts)."""
        if self._is_multi_machine:
            self._verify_ssh_connectivity()
            self._start_multi_machine()
        else:
            self._start_single_machine()

        # Initialize observer and fault injector
        cm_log_parser = LogParser(self.cm.log_path, self.cm.host, self._ssh)
        ds_log_parsers = {
            ds.addr_str: LogParser(ds.log_path, ds.host, self._ssh)
            for ds in self.data_servers
        }

        self.observer = ClusterObserver(
            admin_client=self._admin_client,
            cm_handle=self.cm,
            cm_log_parser=cm_log_parser,
            ds_log_parsers=ds_log_parsers,
            use_admin_rpc=self._use_rpc,
        )

        self.fault_injector = FaultInjector(self._process_manager, self._ssh)
        logger.info("Cluster started: CM pid=%d on %s, %d DS",
                     self.cm.pid, self.cm.host, len(self.data_servers))

    def _start_single_machine(self) -> None:
        """Start all processes on localhost."""
        logger.info("Starting single-machine cluster: %d DS, %d shards",
                     self.config.num_data_servers, self.config.shard_total_num)

        self.cm = self._process_manager.start_cluster_manager(
            host="127.0.0.1",
            build_dir=self._default_build_dir,
            log_dir=self._default_log_dir,
            cm_cluster_init_grace_period_inSecs=self.config.cm_cluster_init_grace_period_inSecs,
            cm_heartbeat_timeout_inSecs=self.config.cm_heartbeat_timeout_inSecs,
            cm_heartbeat_bg_scan_interval_inSecs=self.config.cm_heartbeat_bg_scan_interval_inSecs,
            shard_total_num=self.config.shard_total_num,
            cm_deferred_reshard_enabled=self.config.cm_deferred_reshard_enabled,
            cm_deferred_reshard_window_inSecs=self.config.cm_deferred_reshard_window_inSecs,
        )

        time.sleep(1)

        for i in range(self.config.num_data_servers):
            logical_id = f"{self.config.ds_logical_node_id_prefix}-{i}"
            ds = self._process_manager.start_data_server(
                cm_ip=self.cm.ip,
                cm_inter_port=self.cm.ports["inter"],
                host="127.0.0.1",
                build_dir=self._default_build_dir,
                log_dir=self._default_log_dir,
                heartbeat_cooldown_sec=self.config.heartbeat_cooldown_sec,
                register_cooldown_sec=self.config.register_cooldown_sec,
                cm_hb_tolerance_count=self.config.cm_hb_tolerance_count,
                cm_connect_retry_interval_sec=self.config.cm_connect_retry_interval_sec,
                memory_limit_bytes=self.config.memory_limit_bytes,
                ds_logical_node_id=logical_id,
            )
            self.data_servers.append(ds)
            time.sleep(0.2)

    def _start_multi_machine(self) -> None:
        """Start CM and DS on their designated remote hosts."""
        cm_host = self.config.cm_host
        ds_hosts = self.config.ds_hosts

        logger.info("Starting multi-machine cluster: CM on %s, %d DS on %d hosts",
                     cm_host.ip, self.config.num_data_servers, len(ds_hosts))

        # Start CM on its designated host
        self.cm = self._process_manager.start_cluster_manager(
            host=cm_host.ip,
            ip=cm_host.ip,
            build_dir=cm_host.build_dir or self._default_build_dir,
            log_dir=cm_host.log_dir,
            cm_cluster_init_grace_period_inSecs=self.config.cm_cluster_init_grace_period_inSecs,
            cm_heartbeat_timeout_inSecs=self.config.cm_heartbeat_timeout_inSecs,
            cm_heartbeat_bg_scan_interval_inSecs=self.config.cm_heartbeat_bg_scan_interval_inSecs,
            shard_total_num=self.config.shard_total_num,
            cm_deferred_reshard_enabled=self.config.cm_deferred_reshard_enabled,
            cm_deferred_reshard_window_inSecs=self.config.cm_deferred_reshard_window_inSecs,
        )

        time.sleep(1)

        # Distribute DS across ds_hosts in round-robin
        for i in range(self.config.num_data_servers):
            host_cfg = ds_hosts[i % len(ds_hosts)]
            logical_id = f"{self.config.ds_logical_node_id_prefix}-{i}"
            ds = self._process_manager.start_data_server(
                cm_ip=self.cm.ip,
                cm_inter_port=self.cm.ports["inter"],
                host=host_cfg.ip,
                ip=host_cfg.ip,
                build_dir=host_cfg.build_dir or self._default_build_dir,
                log_dir=host_cfg.log_dir,
                heartbeat_cooldown_sec=self.config.heartbeat_cooldown_sec,
                register_cooldown_sec=self.config.register_cooldown_sec,
                cm_hb_tolerance_count=self.config.cm_hb_tolerance_count,
                cm_connect_retry_interval_sec=self.config.cm_connect_retry_interval_sec,
                memory_limit_bytes=self.config.memory_limit_bytes,
                ds_logical_node_id=logical_id,
            )
            self.data_servers.append(ds)
            time.sleep(0.2)

    def wait_ready(self, timeout: float = 120) -> None:
        """Wait until grace period passes and all DS have registered."""
        grace_wait = self.config.cm_cluster_init_grace_period_inSecs + 2
        logger.info("Waiting %.0fs for grace period to expire...", grace_wait)
        time.sleep(grace_wait)

        # Verify CM is still alive
        if not self._process_manager.is_alive(self.cm):
            cm_log = self._process_manager.get_log(self.cm)
            raise RuntimeError(f"CM died during startup. Log:\n{cm_log[-2000:]}")

        # Verify all DS are alive
        for ds in self.data_servers:
            if not self._process_manager.is_alive(ds):
                ds_log = self._process_manager.get_log(ds)
                raise RuntimeError(
                    f"DS[{ds.index}] died during startup. Log:\n{ds_log[-2000:]}"
                )

        # Try to verify via admin RPC if available
        if self._use_rpc:
            if not self.observer.wait_for_node_count(
                self.config.num_data_servers, timeout=timeout - grace_wait
            ):
                logger.warning("Not all DS registered within timeout (via RPC)")
        else:
            cm_log = LogParser(self.cm.log_path, self.cm.host, self._ssh)
            events = cm_log.find_handshake_events()
            logger.info("Found %d handshake events in CM log", len(events))

        logger.info("Cluster ready")

    def teardown(self) -> None:
        """Kill all processes across all hosts, collect logs."""
        logger.info("Tearing down cluster...")

        if self.cm:
            cm_log = self._process_manager.get_log(self.cm)
            if cm_log:
                logger.debug("CM log (last 500 chars): %s", cm_log[-500:])

        self._process_manager.cleanup_all()
        self._port_allocator.release_all()
        self.cm = None
        self.data_servers.clear()
        logger.info("Cluster teardown complete")

    def restart_cm(self) -> ProcessHandle:
        """Kill CM and restart it with the same ports. Returns new handle."""
        if self.cm is None:
            raise RuntimeError("No CM to restart")
        new_cm = self._process_manager.restart(self.cm)
        self.cm = new_cm

        cm_log_parser = LogParser(new_cm.log_path, new_cm.host, self._ssh)
        self.observer = ClusterObserver(
            admin_client=self._admin_client,
            cm_handle=new_cm,
            cm_log_parser=cm_log_parser,
            use_admin_rpc=self._use_rpc,
        )
        logger.info("CM restarted: pid=%d on %s", new_cm.pid, new_cm.host)
        return new_cm

    def add_data_server(self, ports: dict[str, int] | None = None,
                        host: str | None = None,
                        ds_logical_node_id: str = "") -> ProcessHandle:
        """Dynamically add a new DS to the running cluster.

        Args:
            ports: Specific ports to use (e.g., for restart with same ports).
            host: Host to start the DS on. Defaults to round-robin from ds_hosts
                  in multi-machine mode, or localhost in single-machine mode.
        """
        if self.cm is None:
            raise RuntimeError("No CM running")

        if host is None:
            if self._is_multi_machine and self.config.ds_hosts:
                # Round-robin across ds_hosts
                idx = len(self.data_servers) % len(self.config.ds_hosts)
                host_cfg = self.config.ds_hosts[idx]
                host = host_cfg.ip
                build_dir = host_cfg.build_dir or self._default_build_dir
                log_dir = host_cfg.log_dir
            else:
                host = "127.0.0.1"
                build_dir = self._default_build_dir
                log_dir = self._default_log_dir
        else:
            # Find matching host config
            build_dir = self._default_build_dir
            log_dir = self._default_log_dir
            for hc in self.config.ds_hosts:
                if hc.ip == host:
                    build_dir = hc.build_dir or self._default_build_dir
                    log_dir = hc.log_dir
                    break

        ds = self._process_manager.start_data_server(
            cm_ip=self.cm.ip,
            cm_inter_port=self.cm.ports["inter"],
            host=host,
            ip=host,
            ports=ports,
            build_dir=build_dir,
            log_dir=log_dir,
            heartbeat_cooldown_sec=self.config.heartbeat_cooldown_sec,
            register_cooldown_sec=self.config.register_cooldown_sec,
            cm_hb_tolerance_count=self.config.cm_hb_tolerance_count,
            cm_connect_retry_interval_sec=self.config.cm_connect_retry_interval_sec,
            memory_limit_bytes=self.config.memory_limit_bytes,
            ds_logical_node_id=ds_logical_node_id,
        )
        self.data_servers.append(ds)
        return ds

    def get_ds_handle(self, index: int) -> ProcessHandle:
        """Get DS handle by index."""
        for ds in self.data_servers:
            if ds.index == index:
                return ds
        raise IndexError(f"No DS with index {index}")

    @property
    def process_manager(self) -> ProcessManager:
        return self._process_manager
