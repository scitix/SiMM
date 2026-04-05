"""Unit tests for port_allocator.py — local port allocation."""

import socket

from framework.port_allocator import PortAllocator


class TestPortAllocator:

    def test_allocate_single_port(self):
        pa = PortAllocator()
        ports = pa.allocate(1)
        assert len(ports) == 1
        assert 1024 < ports[0] < 65536

    def test_allocate_multiple_ports_unique(self):
        pa = PortAllocator()
        ports = pa.allocate(10)
        assert len(ports) == 10
        assert len(set(ports)) == 10  # all unique

    def test_no_collision_across_calls(self):
        pa = PortAllocator()
        ports1 = pa.allocate(5)
        ports2 = pa.allocate(5)
        all_ports = ports1 + ports2
        assert len(set(all_ports)) == 10

    def test_allocate_cm_ports(self):
        pa = PortAllocator()
        ports = pa.allocate_cm_ports()
        assert set(ports.keys()) == {"intra", "inter", "admin"}
        assert len(set(ports.values())) == 3  # all different

    def test_allocate_ds_ports(self):
        pa = PortAllocator()
        ports = pa.allocate_ds_ports()
        assert set(ports.keys()) == {"io", "mgt", "admin"}
        assert len(set(ports.values())) == 3

    def test_cm_and_ds_ports_no_overlap(self):
        pa = PortAllocator()
        cm = pa.allocate_cm_ports()
        ds = pa.allocate_ds_ports()
        cm_set = set(cm.values())
        ds_set = set(ds.values())
        assert cm_set.isdisjoint(ds_set)

    def test_release_all(self):
        pa = PortAllocator()
        pa.allocate(5)
        pa.release_all()
        # After release, internal tracking is cleared
        assert pa._allocated == {}

    def test_ports_are_bindable(self):
        """Allocated ports should be currently free (best-effort check)."""
        pa = PortAllocator()
        ports = pa.allocate(3)
        for port in ports:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                # Should not raise — port was free when allocated
                s.bind(("", port))
