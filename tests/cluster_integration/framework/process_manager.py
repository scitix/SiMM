"""Process lifecycle management for CM and DS binaries — supports local and remote hosts.

In multi-machine mode, processes are started on remote hosts via SSH (nohup).
The test runner (node A) sends commands over SSH to start/stop/signal processes
on the CM/DS nodes.
"""

import logging
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from .port_allocator import PortAllocator
from .ssh_executor import SshExecutor

logger = logging.getLogger(__name__)


@dataclass
class ProcessHandle:
    """Tracks a single spawned CM or DS process."""
    pid: int
    process: subprocess.Popen | None   # None for remote processes
    role: str                           # "cm" or "ds"
    index: int                          # ds index (0, 1, 2, ...), 0 for cm
    ip: str                             # IP of the host running this process
    host: str                           # host address for SSH (may == ip)
    ports: dict[str, int]
    log_path: str                       # path on the target host (str, not Path)
    start_time: float
    cmd_args: list[str] = field(default_factory=list)  # for restart
    extra_flags: dict = field(default_factory=dict)
    build_dir: str = ""                 # binary dir on the target host
    admin_name: str = ""                # pod name for UDS admin socket

    @property
    def addr_str(self) -> str:
        """Address string used by CM to identify this node."""
        if self.role == "ds":
            return f"{self.ip}:{self.ports['io']}"
        return f"{self.ip}:{self.ports['inter']}"


class ProcessManager:
    """Manages CM and DS process lifecycle across local and remote hosts."""

    def __init__(self, default_build_dir: str, default_log_dir: str,
                 port_allocator: PortAllocator, ssh_executor: SshExecutor):
        self._default_build_dir = str(default_build_dir)
        self._default_log_dir = str(default_log_dir)
        self._port_allocator = port_allocator
        self._ssh = ssh_executor
        self._handles: list[ProcessHandle] = []
        self._ds_index = 0

    def _resolve_binary(self, build_dir: str, name: str) -> str:
        """Return full path to a binary on the target host."""
        return f"{build_dir}/{name}"

    def start_cluster_manager(
        self,
        host: str = "127.0.0.1",
        ip: str | None = None,
        ports: dict[str, int] | None = None,
        build_dir: str | None = None,
        log_dir: str | None = None,
        cm_cluster_init_grace_period_inSecs: int = 5,
        cm_heartbeat_timeout_inSecs: int = 10,
        cm_heartbeat_bg_scan_interval_inSecs: int = 2,
        shard_total_num: int = 64,
        cm_deferred_reshard_enabled: bool = True,
        cm_deferred_reshard_window_inSecs: int = 120,
        admin_name: str = "cm",
        extra_flags: dict | None = None,
    ) -> ProcessHandle:
        if ip is None:
            ip = host
        build_dir = build_dir or self._default_build_dir
        log_dir = log_dir or self._default_log_dir

        if ports is None:
            ports = self._port_allocator.allocate_cm_ports(host=host)

        log_path = f"{log_dir}/cm.log"
        default_log_path = f"{log_dir}/cm_default.log"
        cm_binary = self._resolve_binary(build_dir, "cluster_manager")

        cmd_parts = [
            cm_binary,
            f"--cm_rpc_intra_port={ports['intra']}",
            f"--cm_rpc_inter_port={ports['inter']}",
            f"--cm_rpc_admin_port={ports['admin']}",
            f"--cm_cluster_init_grace_period_inSecs={cm_cluster_init_grace_period_inSecs}",
            f"--cm_heartbeat_timeout_inSecs={cm_heartbeat_timeout_inSecs}",
            f"--cm_heartbeat_bg_scan_interval_inSecs={cm_heartbeat_bg_scan_interval_inSecs}",
            f"--shard_total_num={shard_total_num}",
            f"--cm_deferred_reshard_enabled={'true' if cm_deferred_reshard_enabled else 'false'}",
            f"--cm_deferred_reshard_window_inSecs={cm_deferred_reshard_window_inSecs}",
            f"--cm_log_file={log_path}",
            f"--default_logging_file={default_log_path}",
        ]
        if extra_flags:
            for k, v in extra_flags.items():
                cmd_parts.append(f"--{k}={v}")

        cmd_str = " ".join(cmd_parts)
        logger.info("Starting CM on %s: %s", host, cmd_str)

        # Ensure log directory exists on target host
        self._ssh.run(host, f"mkdir -p {log_dir}", timeout=5, check=False)

        pid = self._start_process(host, cmd_str)

        handle = ProcessHandle(
            pid=pid,
            process=None,  # remote — no Popen object
            role="cm",
            index=0,
            ip=ip,
            host=host,
            ports=ports,
            log_path=log_path,
            start_time=time.time(),
            cmd_args=cmd_parts,
            extra_flags=extra_flags or {},
            build_dir=build_dir,
            admin_name=admin_name,
        )
        self._handles.append(handle)
        logger.info("CM started on %s: pid=%d ports=%s", host, pid, ports)
        return handle

    def start_data_server(
        self,
        cm_ip: str,
        cm_inter_port: int,
        host: str = "127.0.0.1",
        ip: str | None = None,
        ports: dict[str, int] | None = None,
        build_dir: str | None = None,
        log_dir: str | None = None,
        heartbeat_cooldown_sec: int = 2,
        register_cooldown_sec: int = 2,
        memory_limit_bytes: int = 1 << 30,
        ds_logical_node_id: str = "",
        extra_flags: dict | None = None,
    ) -> ProcessHandle:
        if ip is None:
            ip = host
        build_dir = build_dir or self._default_build_dir
        log_dir = log_dir or self._default_log_dir

        if ports is None:
            ports = self._port_allocator.allocate_ds_ports(host=host)

        idx = self._ds_index
        self._ds_index += 1

        log_path = f"{log_dir}/ds_{idx}.log"
        default_log_path = f"{log_dir}/ds_{idx}_default.log"
        ds_binary = self._resolve_binary(build_dir, "data_server")

        cmd_parts = [
            ds_binary,
            f"--cm_primary_node_ip={cm_ip}",
            f"--cm_rpc_inter_port={cm_inter_port}",
            f"--io_service_port={ports['io']}",
            f"--mgt_service_port={ports['mgt']}",
            f"--ds_rpc_admin_port={ports['admin']}",
            f"--heartbeat_cooldown_sec={heartbeat_cooldown_sec}",
            f"--register_cooldown_sec={register_cooldown_sec}",
            f"--memory_limit_bytes={memory_limit_bytes}",
            f"--ds_log_file={log_path}",
            f"--default_logging_file={default_log_path}",
        ]
        if ds_logical_node_id:
            cmd_parts.append(f"--ds_logical_node_id={ds_logical_node_id}")
        if extra_flags:
            for k, v in extra_flags.items():
                cmd_parts.append(f"--{k}={v}")

        cmd_str = " ".join(cmd_parts)
        logger.info("Starting DS[%d] on %s: %s", idx, host, cmd_str)

        self._ssh.run(host, f"mkdir -p {log_dir}", timeout=5, check=False)
        pid = self._start_process(host, cmd_str)

        handle = ProcessHandle(
            pid=pid,
            process=None,
            role="ds",
            index=idx,
            ip=ip,
            host=host,
            ports=ports,
            log_path=log_path,
            start_time=time.time(),
            cmd_args=cmd_parts,
            extra_flags=extra_flags or {},
            build_dir=build_dir,
            admin_name=ds_logical_node_id or "ds",
        )
        self._handles.append(handle)
        logger.info("DS[%d] started on %s: pid=%d ports=%s", idx, host, pid, ports)
        return handle

    def _start_process(self, host: str, cmd: str) -> int:
        """Start a process on host. Returns remote PID."""
        pid = self._ssh.run_background(host, cmd)
        if pid is None:
            raise RuntimeError(f"Failed to start process on {host}: {cmd}")
        return pid

    def kill(self, handle: ProcessHandle, sig: int = signal.SIGKILL) -> None:
        """Send a signal to the process (local or remote)."""
        if not self.is_alive(handle):
            logger.warning("Process %d on %s already dead", handle.pid, handle.host)
            return
        success = self._ssh.send_signal(handle.host, handle.pid, sig)
        if success:
            logger.info("Sent signal %d to pid %d on %s (%s[%d])",
                        sig, handle.pid, handle.host, handle.role, handle.index)
        else:
            logger.warning("Failed to send signal %d to pid %d on %s",
                           sig, handle.pid, handle.host)

    def stop(self, handle: ProcessHandle, timeout: float = 10) -> None:
        """Send SIGTERM and wait for graceful exit."""
        self.kill(handle, signal.SIGTERM)
        self.wait_for_exit(handle, timeout=timeout)

    def freeze(self, handle: ProcessHandle) -> None:
        """Send SIGSTOP — simulates process hang."""
        self.kill(handle, signal.SIGSTOP)

    def unfreeze(self, handle: ProcessHandle) -> None:
        """Send SIGCONT — resumes frozen process."""
        self.kill(handle, signal.SIGCONT)

    def is_alive(self, handle: ProcessHandle) -> bool:
        """Check if process is still running (local or remote)."""
        return self._ssh.is_process_alive(handle.host, handle.pid)

    def wait_for_exit(self, handle: ProcessHandle, timeout: float = 30) -> bool:
        """Wait for process to exit. Returns True if exited, False on timeout."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self.is_alive(handle):
                return True
            time.sleep(0.5)
        logger.warning("Process %d on %s did not exit within %.1fs",
                       handle.pid, handle.host, timeout)
        return False

    def restart(self, handle: ProcessHandle) -> ProcessHandle:
        """Kill and restart a process with the same arguments."""
        if self.is_alive(handle):
            self.kill(handle, signal.SIGKILL)
            self.wait_for_exit(handle, timeout=5)

        # Remove old handle
        self._handles = [h for h in self._handles if h.pid != handle.pid]

        cmd_str = " ".join(handle.cmd_args)
        logger.info("Restarting %s[%d] on %s with same args",
                     handle.role, handle.index, handle.host)
        pid = self._start_process(handle.host, cmd_str)

        new_handle = ProcessHandle(
            pid=pid,
            process=None,
            role=handle.role,
            index=handle.index,
            ip=handle.ip,
            host=handle.host,
            ports=handle.ports,
            log_path=handle.log_path,
            start_time=time.time(),
            cmd_args=handle.cmd_args,
            extra_flags=handle.extra_flags,
            build_dir=handle.build_dir,
            admin_name=handle.admin_name,
        )
        self._handles.append(new_handle)
        logger.info("%s[%d] restarted on %s: pid=%d",
                    handle.role, handle.index, handle.host, pid)
        return new_handle

    def get_log(self, handle: ProcessHandle) -> str:
        """Return log file contents from the target host."""
        return self._ssh.read_file(handle.host, handle.log_path)

    def cleanup_all(self) -> None:
        """Kill all managed processes across all hosts. Called in fixture teardown."""
        for handle in self._handles:
            if self.is_alive(handle):
                # Unfreeze first in case it was SIGSTOP'd
                self._ssh.send_signal(handle.host, handle.pid, signal.SIGCONT)
                self._ssh.send_signal(handle.host, handle.pid, signal.SIGKILL)

        # Wait briefly for all to die
        deadline = time.time() + 5
        while time.time() < deadline:
            if all(not self.is_alive(h) for h in self._handles):
                break
            time.sleep(0.5)

        self._handles.clear()
        logger.info("All processes cleaned up")
