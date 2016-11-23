package clusterinfo

import "testing"

func TestHostNameAddresses(t *testing.T) {
	p := &Producer{
		BroadcastAddress: "host.domain.com",
		TCPPort:          4150,
		HTTPPort:         4151,
	}

	if p.HTTPAddress() != "host.domain.com:4151" {
		t.Errorf("Incorrect HTTPAddress: %s", p.HTTPAddress())
	}
	if p.TCPAddress() != "host.domain.com:4150" {
		t.Errorf("Incorrect TCPAddress: %s", p.TCPAddress())
	}
}

func TestIPv4Addresses(t *testing.T) {
	p := &Producer{
		BroadcastAddress: "192.168.1.17",
		TCPPort:          4150,
		HTTPPort:         4151,
	}

	if p.HTTPAddress() != "192.168.1.17:4151" {
		t.Errorf("Incorrect IPv4 HTTPAddress: %s", p.HTTPAddress())
	}
	if p.TCPAddress() != "192.168.1.17:4150" {
		t.Errorf("Incorrect IPv4 TCPAddress: %s", p.TCPAddress())
	}
}

func TestIPv6Addresses(t *testing.T) {
	p := &Producer{
		BroadcastAddress: "fd4a:622f:d2f2::1",
		TCPPort:          4150,
		HTTPPort:         4151,
	}
	if p.HTTPAddress() != "[fd4a:622f:d2f2::1]:4151" {
		t.Errorf("Incorrect IPv6 HTTPAddress: %s", p.HTTPAddress())
	}
	if p.TCPAddress() != "[fd4a:622f:d2f2::1]:4150" {
		t.Errorf("Incorrect IPv6 TCPAddress: %s", p.TCPAddress())
	}
}
