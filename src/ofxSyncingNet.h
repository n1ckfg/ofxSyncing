#pragma once

#include <string>
#include <vector>

// Small POSIX networking helpers. IPv4 only: that's what the Pis, multicast
// discovery and Tailscale's 100.x addresses all use.

namespace ofxSyncing {
namespace net {

std::string hostName();

// The interface's IPv4 address, or "" if it has none.
std::string interfaceAddress(const std::string & ifname);

// 0 if there's no such interface.
unsigned interfaceIndex(const std::string & ifname);

// Every IPv4 address on this machine, loopback included.
std::vector<std::string> localAddresses();
bool isLocalAddress(const std::string & ip);

// Other interfaces with an address on ifname's subnet. On a Pi with eth0 and
// wlan0 on the same subnet, traffic can go out either one.
std::vector<std::string> interfacesSharingSubnet(const std::string & ifname);

// IPv4 addresses for a hostname or dotted quad, with plain getaddrinfo (so
// .local names resolve through libnss-mdns). May block for seconds.
std::vector<std::string> resolve(const std::string & host);

} // namespace net
} // namespace ofxSyncing
