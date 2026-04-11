"""Root pytest conftest with shared fixtures for distributed protocol tests.

Supports both single-machine and multi-machine modes:
- Single-machine (default): all processes on localhost
- Multi-machine: set SIMM_CLUSTER_CONFIG env var to a YAML file with host topology

Example multi-machine YAML:
    cluster:
      hosts:
        cm:
          ip: "10.0.0.1"
          binary_dir: "/opt/simm/build/release/bin"
          log_dir: "/tmp/simm_test_logs"
        ds:
          - ip: "10.0.0.2"
            binary_dir: "/opt/simm/build/release/bin"
          - ip: "10.0.0.3"
          - ip: "10.0.0.4"
      ssh:
        user: "root"
        port: 22
"""

import logging
import os
from pathlib import Path

import pytest

from framework.cluster import SimmCluster
from framework.config import ClusterConfig, dict_to_cluster_config, load_yaml

logger = logging.getLogger(__name__)


def detect_root() -> bool:
    return os.geteuid() == 0


def _load_multi_machine_config() -> dict | None:
    """Load cluster topology from SIMM_CLUSTER_CONFIG env var."""
    config_path = os.environ.get("SIMM_CLUSTER_CONFIG")
    if not config_path:
        return None
    p = Path(config_path)
    if not p.exists():
        logger.warning("SIMM_CLUSTER_CONFIG=%s does not exist, falling back to single-machine",
                        config_path)
        return None
    return load_yaml(p)


def _make_config(
    num_ds: int = 3,
    shard_total_num: int = 64,
    cm_cluster_init_grace_period_inSecs: int = 5,
    cm_heartbeat_timeout_inSecs: int = 10,
    cm_heartbeat_bg_scan_interval_inSecs: int = 2,
    heartbeat_cooldown_sec: int = 2,
    register_cooldown_sec: int = 2,
    memory_limit_bytes: int = 1 << 30,
) -> ClusterConfig:
    """Create a ClusterConfig, applying multi-machine host topology if configured."""
    ext_data = _load_multi_machine_config()

    if ext_data:
        base = dict_to_cluster_config(ext_data)
        base.num_data_servers = num_ds
        base.shard_total_num = shard_total_num
        base.cm_cluster_init_grace_period_inSecs = cm_cluster_init_grace_period_inSecs
        base.cm_heartbeat_timeout_inSecs = cm_heartbeat_timeout_inSecs
        base.cm_heartbeat_bg_scan_interval_inSecs = cm_heartbeat_bg_scan_interval_inSecs
        base.heartbeat_cooldown_sec = heartbeat_cooldown_sec
        base.register_cooldown_sec = register_cooldown_sec
        base.memory_limit_bytes = memory_limit_bytes
        return base

    return ClusterConfig(
        num_data_servers=num_ds,
        shard_total_num=shard_total_num,
        cm_cluster_init_grace_period_inSecs=cm_cluster_init_grace_period_inSecs,
        cm_heartbeat_timeout_inSecs=cm_heartbeat_timeout_inSecs,
        cm_heartbeat_bg_scan_interval_inSecs=cm_heartbeat_bg_scan_interval_inSecs,
        heartbeat_cooldown_sec=heartbeat_cooldown_sec,
        register_cooldown_sec=register_cooldown_sec,
        memory_limit_bytes=memory_limit_bytes,
    )


@pytest.fixture(scope="session")
def has_root():
    return detect_root()


@pytest.fixture(scope="session")
def binary_dir():
    """Resolve the build directory containing SiMM binaries (for local/single-machine)."""
    env_dir = os.environ.get("SIMM_BUILD_DIR")
    if env_dir:
        d = Path(env_dir)
        if d.exists():
            return d

    # Default: look relative to this file
    simm_root = Path(__file__).parents[2]  # tests/protocol_test -> SiMM root
    for mode in ["release", "relwithdeb", "debug"]:
        d = simm_root / "build" / mode / "bin"
        if (d / "cluster_manager").exists():
            return d

    # In multi-machine mode, binaries may only exist on remote hosts.
    # cluster.py uses per-host binary_dir from YAML config, so local path is unused.
    if os.environ.get("SIMM_CLUSTER_CONFIG"):
        return None

    # pytest.skip raises Skipped exception — execution never reaches implicit return
    pytest.skip("SiMM binaries not found. Set SIMM_BUILD_DIR or build with ./build.sh")
    return Path("/dev/null")


@pytest.fixture
def cluster_small(tmp_path, binary_dir):
    """1 CM + 3 DS cluster with fast heartbeats."""
    config = _make_config(num_ds=3, shard_total_num=64)
    cluster = SimmCluster(
        config,
        log_dir=tmp_path / "logs" if not config.cm_host else None,
        binary_dir=binary_dir if not config.cm_host else None,
    )
    cluster.start()
    cluster.wait_ready()
    yield cluster
    cluster.teardown()


@pytest.fixture
def cluster_medium(tmp_path, binary_dir):
    """1 CM + 6 DS cluster."""
    config = _make_config(num_ds=6, cm_cluster_init_grace_period_inSecs=8)
    cluster = SimmCluster(
        config,
        log_dir=tmp_path / "logs" if not config.cm_host else None,
        binary_dir=binary_dir if not config.cm_host else None,
    )
    cluster.start()
    cluster.wait_ready()
    yield cluster
    cluster.teardown()


@pytest.fixture
def cluster_from_config(tmp_path, binary_dir):
    """
    Factory fixture: creates a cluster from a custom ClusterConfig.
    Usage:
        def test_custom(cluster_from_config):
            cluster = cluster_from_config(ClusterConfig(num_data_servers=4, ...))
    """
    clusters = []

    def _factory(config: ClusterConfig) -> SimmCluster:
        cluster = SimmCluster(
            config,
            log_dir=tmp_path / "logs" if not config.cm_host else None,
            binary_dir=binary_dir if not config.cm_host else None,
        )
        cluster.start()
        cluster.wait_ready()
        clusters.append(cluster)
        return cluster

    yield _factory

    for c in clusters:
        c.teardown()
