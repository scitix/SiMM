"""SSH command execution layer for multi-machine testing.

Provides a unified interface for running commands either locally or on remote
hosts via passwordless SSH. The test runner (node A) uses this to start/stop
processes, send signals, read logs, and manage iptables on remote CM/DS nodes.
"""

import logging
import subprocess
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SshConfig:
    """SSH connection settings."""
    user: str = ""                 # SSH user; empty = current user
    port: int = 22
    connect_timeout: int = 5
    options: dict[str, str] | None = None  # extra SSH options


class SshExecutor:
    """
    Executes shell commands on local or remote hosts via SSH.

    Usage:
        exe = SshExecutor()
        # Local
        result = exe.run("127.0.0.1", "ls /tmp")
        # Remote
        result = exe.run("10.0.0.2", "kill -9 12345")
    """

    # Hosts treated as local (no SSH needed)
    LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1"}

    def __init__(self, ssh_config: SshConfig | None = None,
                 local_host: str | None = None):
        """
        Args:
            ssh_config: SSH connection settings for remote hosts.
            local_host: The IP/hostname of the machine running tests.
                        Commands targeting this host run locally without SSH.
                        If None, only 127.0.0.1/localhost are treated as local.
        """
        self._ssh_config = ssh_config or SshConfig()
        self._local_hosts = set(self.LOCAL_HOSTS)
        if local_host:
            self._local_hosts.add(local_host)

    def is_local(self, host: str) -> bool:
        """Check if host is the local machine."""
        return host in self._local_hosts

    def _build_ssh_cmd(self, host: str) -> list[str]:
        """Build the SSH prefix command."""
        cmd = ["ssh"]
        # Common options for non-interactive batch mode
        cmd += [
            "-o", "BatchMode=yes",
            "-o", "StrictHostKeyChecking=no",
            "-o", f"ConnectTimeout={self._ssh_config.connect_timeout}",
        ]
        if self._ssh_config.port != 22:
            cmd += ["-p", str(self._ssh_config.port)]
        if self._ssh_config.options:
            for k, v in self._ssh_config.options.items():
                cmd += ["-o", f"{k}={v}"]

        target = host
        if self._ssh_config.user:
            target = f"{self._ssh_config.user}@{host}"
        cmd.append(target)
        return cmd

    def run(self, host: str, command: str,
            timeout: float = 30, check: bool = True) -> subprocess.CompletedProcess:
        """
        Run a shell command on the target host.

        Args:
            host: Target hostname or IP.
            command: Shell command to execute.
            timeout: Timeout in seconds.
            check: If True, raise on non-zero exit code.

        Returns:
            CompletedProcess with stdout/stderr.

        Raises:
            subprocess.CalledProcessError: If check=True and command fails.
            SshError: If SSH connection fails.
        """
        if self.is_local(host):
            full_cmd = ["bash", "-c", command]
        else:
            full_cmd = self._build_ssh_cmd(host) + [command]

        logger.debug("Exec on %s: %s", host, command)
        try:
            result = subprocess.run(
                full_cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            if check and result.returncode != 0:
                raise subprocess.CalledProcessError(
                    result.returncode, full_cmd,
                    output=result.stdout, stderr=result.stderr,
                )
            return result
        except subprocess.TimeoutExpired:
            raise SshError(f"Command timed out on {host}: {command}")

    def run_background(self, host: str, command: str) -> int | None:
        """
        Start a command in the background on a remote host via nohup.
        Returns the remote PID if parseable.

        The command is launched with nohup and stdout/stderr redirected,
        so it survives the SSH session closing.
        """
        # Wrap command to background and echo PID
        bg_cmd = f"nohup {command} > /dev/null 2>&1 & echo $!"
        result = self.run(host, bg_cmd, timeout=10)
        pid_str = result.stdout.strip()
        if pid_str.isdigit():
            pid = int(pid_str)
            logger.info("Background process started on %s: pid=%d", host, pid)
            return pid
        logger.warning("Could not parse PID from: %s", pid_str)
        return None

    def send_signal(self, host: str, pid: int, sig: int) -> bool:
        """Send a signal to a remote process."""
        try:
            self.run(host, f"kill -{sig} {pid}", timeout=5)
            return True
        except (subprocess.CalledProcessError, SshError):
            logger.warning("Failed to send signal %d to pid %d on %s", sig, pid, host)
            return False

    def is_process_alive(self, host: str, pid: int) -> bool:
        """Check if a remote process is still running."""
        try:
            result = self.run(host, f"kill -0 {pid}", timeout=5, check=False)
            return result.returncode == 0
        except SshError as e:
            logger.debug("is_process_alive check failed on %s pid=%d: %s", host, pid, e)
            return False

    def read_file(self, host: str, path: str) -> str:
        """Read a file from a remote host."""
        try:
            result = self.run(host, f"cat {path}", timeout=15, check=False)
            return result.stdout if result.returncode == 0 else ""
        except SshError as e:
            logger.debug("read_file failed on %s path=%s: %s", host, path, e)
            return ""

    def read_file_tail(self, host: str, path: str, offset: int = 0) -> tuple[str, int]:
        """
        Read new content from a file starting at byte offset.
        Returns (new_content, new_offset).
        """
        # Use dd to skip to offset, then read the rest
        cmd = (f"test -f {path} && "
               f"dd if={path} bs=1 skip={offset} 2>/dev/null; "
               f"wc -c < {path} 2>/dev/null || echo 0")
        try:
            result = self.run(host, cmd, timeout=15, check=False)
            lines = result.stdout.rsplit("\n", 1)
            if len(lines) == 2:
                content = lines[0]
                try:
                    new_offset = int(lines[1].strip())
                except ValueError:
                    new_offset = offset + len(content)
            else:
                content = result.stdout
                new_offset = offset + len(content)
            return content, new_offset
        except SshError as e:
            logger.debug("read_file_tail failed on %s path=%s: %s", host, path, e)
            return "", offset

    def file_exists(self, host: str, path: str) -> bool:
        """Check if a file exists on a remote host."""
        try:
            result = self.run(host, f"test -f {path}", timeout=5, check=False)
            return result.returncode == 0
        except SshError as e:
            logger.debug("file_exists check failed on %s path=%s: %s", host, path, e)
            return False

    def find_free_port(self, host: str) -> int | None:
        """Find a free port on a remote host."""
        cmd = ("python3 -c \""
               "import socket; s=socket.socket(); "
               "s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); "
               "s.bind(('', 0)); "
               "print(s.getsockname()[1]); "
               "s.close()\"")
        try:
            result = self.run(host, cmd, timeout=5)
            port_str = result.stdout.strip()
            if port_str.isdigit():
                return int(port_str)
        except (subprocess.CalledProcessError, SshError) as e:
            logger.debug("find_free_port failed on %s: %s", host, e)
        return None

    def run_iptables(self, host: str, args: str) -> bool:
        """Run an iptables command on a remote host."""
        try:
            self.run(host, f"iptables {args}", timeout=5)
            return True
        except (subprocess.CalledProcessError, SshError) as e:
            logger.warning("iptables failed on %s: %s", host, e)
            return False

    def check_connectivity(self, host: str) -> bool:
        """Verify SSH connectivity to a host."""
        try:
            result = self.run(host, "echo ok", timeout=self._ssh_config.connect_timeout + 2,
                              check=False)
            return result.returncode == 0 and "ok" in result.stdout
        except SshError as e:
            logger.debug("check_connectivity failed for %s: %s", host, e)
            return False


class SshError(Exception):
    """Raised when SSH operation fails."""
    pass
