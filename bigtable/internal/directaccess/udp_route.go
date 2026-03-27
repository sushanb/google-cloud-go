package directaccess

import (
	"fmt"
	"net"
	"time"

	btopt "cloud.google.com/go/bigtable/internal/option"
)

// CheckLocalIPv4Routes verifies if the OS has a valid route to the target
// using the specified local IPv4 address as the source.
func CheckLocalIPv4Routes(localIP *net.IP, targetEndpoint string) error {
	srcStr := "default (kernel selected)"
	if localIP != nil {
		srcStr = localIP.String()
	}
	btopt.Debugf(nil, "directaccess: Checking IPv4 kernel route to %s using source %s", targetEndpoint, srcStr)
	return checkRoute("udp4", localIP, targetEndpoint)
}

// CheckLocalIPv6Routes verifies if the OS has a valid route to the target
// using the specified local IPv6 address as the source.
func CheckLocalIPv6Routes(localIP *net.IP, targetEndpoint string) error {
	srcStr := "default (kernel selected)"
	if localIP != nil {
		srcStr = localIP.String()
	}
	btopt.Debugf(nil, "directaccess: Checking IPv6 kernel route to %s using source %s", targetEndpoint, srcStr)
	return checkRoute("udp6", localIP, targetEndpoint)
}

// checkRoute uses a local UDP dial to force the kernel to perform a routing table lookup.
func checkRoute(network string, localIP *net.IP, targetEndpoint string) error {
	dialer := &net.Dialer{
		Timeout: 2 * time.Second,
	}

	// Only explicitly bind the source IP if it is provided
	if localIP != nil {
		dialer.LocalAddr = &net.UDPAddr{IP: *localIP}
	}

	btopt.Debugf(nil, "directaccess: Initiating local UDP dial to test route resolution (%s)", network)
	conn, err := dialer.Dial(network, targetEndpoint)
	if err != nil {
		btopt.Debugf(nil, "directaccess: Kernel route lookup rejected dial: %v", err)
		return fmt.Errorf("kernel route lookup failed for %v -> %s: %w", localIP, targetEndpoint, err)
	}
	defer conn.Close()

	btopt.Debugf(nil, "directaccess: Kernel route lookup successful. Socket safely bound.")
	return nil
}
