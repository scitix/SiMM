"""Dynamic free port allocator for test processes — supports local and remote hosts."""

import socket
import threading

from .ssh_executor import SshExecutor


class PortAllocator:
    """Thread-safe free port allocator using bind-then-close probing.

    Supports allocating ports on both local and remote hosts.
    Remote port probing is done via SSH (python3 one-liner on the remote).
    """

    def __init__(self, ssh_executor: SshExecutor | None = None):
        self._ssh = ssh_executor
        self._lock = threading.Lock()
        # Track allocated ports per host to avoid collisions
        self._allocated: dict[str, set[int]] = {}  # {host: {ports}}

    def _get_host_set(self, host: str) -> set[int]:
        if host not in self._allocated:
            self._allocated[host] = set()
        return self._allocated[host]

    def _find_free_port_local(self) -> int:
        """Find a single free port on the local machine."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(("", 0))
            return s.getsockname()[1]

    def _find_free_port_remote(self, host: str) -> int:
        """Find a single free port on a remote host via SSH."""
        if self._ssh is None:
            raise RuntimeError("SshExecutor required for remote port allocation")
        port = self._ssh.find_free_port(host)
        if port is None:
            raise RuntimeError(f"Failed to find free port on {host}")
        return port

    def _find_free_port(self, host: str) -> int:
        """Find a free port, local or remote."""
        if self._ssh is None or self._ssh.is_local(host):
            return self._find_free_port_local()
        return self._find_free_port_remote(host)

    def allocate(self, count: int = 1, host: str = "127.0.0.1") -> list[int]:
        """Return `count` currently-free ports on `host`, unique within this allocator."""
        ports = []
        with self._lock:
            host_set = self._get_host_set(host)
            for _ in range(count):
                for _ in range(100):
                    port = self._find_free_port(host)
                    if port not in host_set:
                        host_set.add(port)
                        ports.append(port)
                        break
                else:
                    raise RuntimeError(
                        f"Failed to allocate a free port on {host} after 100 attempts"
                    )
        return ports

    def allocate_cm_ports(self, host: str = "127.0.0.1") -> dict[str, int]:
        """Allocate 3 ports for a Cluster Manager on the given host."""
        ports = self.allocate(3, host=host)
        return {"intra": ports[0], "inter": ports[1], "admin": ports[2]}

    def allocate_ds_ports(self, host: str = "127.0.0.1") -> dict[str, int]:
        """Allocate 3 ports for a Data Server on the given host."""
        ports = self.allocate(3, host=host)
        return {"io": ports[0], "mgt": ports[1], "admin": ports[2]}

    def release_all(self):
        with self._lock:
            self._allocated.clear()
