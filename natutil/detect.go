package natutil

import "net"

// NATStatus describes the NAT status of a node.
type NATStatus struct {
	PublicAddr string // observed public IP:port
	IsNATed   bool   // bind addr != public addr
	IsRelay   bool   // node has public IP, can offer relay service
}

// DetectNAT compares the bind address with the reflected address to determine
// NAT status. If the IPs match, the node is not behind NAT and can act as a
// relay. Symmetric NAT detection is deferred to hole-punch failure.
func DetectNAT(bindAddr string, reflectedAddr string) NATStatus {
	if reflectedAddr == "" {
		return NATStatus{}
	}

	bindHost, _, _ := net.SplitHostPort(bindAddr)
	reflectedHost, _, _ := net.SplitHostPort(reflectedAddr)

	bindIP := net.ParseIP(bindHost)
	reflectedIP := net.ParseIP(reflectedHost)

	// If bind is 0.0.0.0 or ::, we can't compare directly — assume NATed
	// unless reflected matches a local interface.
	if bindIP != nil && bindIP.IsUnspecified() {
		return NATStatus{
			PublicAddr: reflectedAddr,
			IsNATed:   !isLocalIP(reflectedIP),
			IsRelay:   isLocalIP(reflectedIP),
		}
	}

	natted := !bindIP.Equal(reflectedIP)
	return NATStatus{
		PublicAddr: reflectedAddr,
		IsNATed:   natted,
		IsRelay:   !natted,
	}
}

func isLocalIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, addr := range addrs {
		ipnet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		if ipnet.IP.Equal(ip) {
			return true
		}
	}
	return false
}
