"""Fault injection for cluster management testing — supports local and remote hosts.

In multi-machine mode, signals are sent via SSH and iptables rules are applied
on the remote hosts where CM/DS processes run.
"""

import logging
import signal
from contextlib import contextmanager

from .process_manager import ProcessHandle, ProcessManager
from .ssh_executor import SshExecutor

logger = logging.getLogger(__name__)


class FaultInjector:
    """Injects faults via OS-level mechanisms (signals, iptables) across hosts."""

    def __init__(self, process_manager: ProcessManager, ssh_executor: SshExecutor):
        self._pm = process_manager
        self._ssh = ssh_executor

    def kill_process(self, handle: ProcessHandle) -> None:
        """SIGKILL — simulates hard crash."""
        logger.info("FAULT: Killing %s[%d] (pid=%d on %s) with SIGKILL",
                     handle.role, handle.index, handle.pid, handle.host)
        self._pm.kill(handle, signal.SIGKILL)

    def graceful_stop(self, handle: ProcessHandle) -> None:
        """SIGTERM — tests graceful shutdown path."""
        logger.info("FAULT: Graceful stop %s[%d] (pid=%d on %s) with SIGTERM",
                     handle.role, handle.index, handle.pid, handle.host)
        self._pm.stop(handle, timeout=10)

    def freeze_process(self, handle: ProcessHandle) -> None:
        """SIGSTOP — simulates process hang/unresponsive."""
        logger.info("FAULT: Freezing %s[%d] (pid=%d on %s) with SIGSTOP",
                     handle.role, handle.index, handle.pid, handle.host)
        self._pm.freeze(handle)

    def unfreeze_process(self, handle: ProcessHandle) -> None:
        """SIGCONT — resumes frozen process."""
        logger.info("FAULT: Unfreezing %s[%d] (pid=%d on %s) with SIGCONT",
                     handle.role, handle.index, handle.pid, handle.host)
        self._pm.unfreeze(handle)

    def kill_multiple(self, handles: list[ProcessHandle],
                      simultaneous: bool = True) -> None:
        """Kill multiple nodes (possibly on different hosts)."""
        logger.info("FAULT: Killing %d processes %s",
                     len(handles), "simultaneously" if simultaneous else "sequentially")
        if simultaneous:
            for h in handles:
                self._ssh.send_signal(h.host, h.pid, signal.SIGKILL)
        else:
            for h in handles:
                self.kill_process(h)

    @contextmanager
    def temporary_partition(
        self,
        handle: ProcessHandle,
        peer_handles: list[ProcessHandle],
        duration: float | None = None,
    ):
        """
        Context manager: create network partition via iptables, yield, then heal.

        In multi-machine mode, iptables rules are applied on both sides:
        - On handle's host: DROP traffic to/from peer IPs
        - On peer hosts: DROP traffic to/from handle's IP

        This creates a true bidirectional partition.
        Requires root privileges on all involved hosts.
        """
        rules = self._build_iptables_rules(handle, peer_handles)
        try:
            self._apply_iptables_rules(rules, add=True)
            logger.info("FAULT: Network partition applied (%d rules)", len(rules))
            yield
        finally:
            self._apply_iptables_rules(rules, add=False)
            logger.info("FAULT: Network partition healed (%d rules removed)", len(rules))

    def _build_iptables_rules(
        self,
        handle: ProcessHandle,
        peer_handles: list[ProcessHandle],
    ) -> list[dict]:
        """Build iptables rules to block traffic between handle and peers.

        For multi-machine, we use IP-based rules (not just port-based) since
        traffic crosses network boundaries. Rules are applied on both sides.
        """
        rules = []

        for peer in peer_handles:
            if handle.host == peer.host:
                # Same host: use port-based rules (original behavior)
                for tp in handle.ports.values():
                    for pp in peer.ports.values():
                        rules.append({
                            "host": handle.host,
                            "chain": "OUTPUT",
                            "args": f"-p tcp --sport {tp} --dport {pp} -j DROP",
                        })
                        rules.append({
                            "host": handle.host,
                            "chain": "OUTPUT",
                            "args": f"-p tcp --sport {pp} --dport {tp} -j DROP",
                        })
            else:
                # Different hosts: block by destination IP + ports
                # On handle's host: block outgoing to peer
                for pp in peer.ports.values():
                    rules.append({
                        "host": handle.host,
                        "chain": "OUTPUT",
                        "args": f"-p tcp -d {peer.ip} --dport {pp} -j DROP",
                    })
                # On handle's host: block incoming from peer
                for tp in handle.ports.values():
                    rules.append({
                        "host": handle.host,
                        "chain": "INPUT",
                        "args": f"-p tcp -s {peer.ip} --dport {tp} -j DROP",
                    })
                # On peer's host: block outgoing to handle
                for tp in handle.ports.values():
                    rules.append({
                        "host": peer.host,
                        "chain": "OUTPUT",
                        "args": f"-p tcp -d {handle.ip} --dport {tp} -j DROP",
                    })
                # On peer's host: block incoming from handle
                for pp in peer.ports.values():
                    rules.append({
                        "host": peer.host,
                        "chain": "INPUT",
                        "args": f"-p tcp -s {handle.ip} --dport {pp} -j DROP",
                    })

        return rules

    def _apply_iptables_rules(self, rules: list[dict], add: bool = True) -> None:
        """Apply or remove iptables rules on the appropriate hosts."""
        action = "-A" if add else "-D"
        for rule in rules:
            host = rule["host"]
            iptables_cmd = f"iptables {action} {rule['chain']} {rule['args']}"
            success = self._ssh.run_iptables(host, f"{action} {rule['chain']} {rule['args']}")
            if not success and add:
                logger.error("iptables rule failed on %s: %s", host, iptables_cmd)
                raise RuntimeError(f"iptables failed on {host}: {iptables_cmd}")
