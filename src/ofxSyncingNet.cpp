#include "ofxSyncingNet.h"

#include <algorithm>
#include <cstring>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace ofxSyncing {
namespace net {

namespace {

std::string toString(const in_addr & addr) {
    char buf[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &addr, buf, sizeof(buf));
    return buf;
}

template <typename F>
void forEachIPv4(F f) {
    ifaddrs * list = nullptr;
    if (getifaddrs(&list) != 0) return;
    for (ifaddrs * i = list; i; i = i->ifa_next) {
        if (!i->ifa_addr || i->ifa_addr->sa_family != AF_INET) continue;
        auto addr = ((sockaddr_in *)i->ifa_addr)->sin_addr;
        in_addr mask{};
        if (i->ifa_netmask) mask = ((sockaddr_in *)i->ifa_netmask)->sin_addr;
        f(std::string(i->ifa_name), addr, mask, (i->ifa_flags & IFF_UP) != 0);
    }
    freeifaddrs(list);
}

} // namespace

std::string hostName() {
    char buf[256] = {0};
    if (gethostname(buf, sizeof(buf) - 1) != 0) return "";
    return buf;
}

std::string interfaceAddress(const std::string & ifname) {
    std::string result;
    forEachIPv4([&](const std::string & name, in_addr addr, in_addr, bool up) {
        if (result.empty() && up && name == ifname) result = toString(addr);
    });
    return result;
}

unsigned interfaceIndex(const std::string & ifname) {
    if (ifname.empty()) return 0;
    return if_nametoindex(ifname.c_str());
}

std::vector<std::string> localAddresses() {
    std::vector<std::string> result;
    forEachIPv4([&](const std::string &, in_addr addr, in_addr, bool) {
        result.push_back(toString(addr));
    });
    return result;
}

bool isLocalAddress(const std::string & ip) {
    // All of 127/8 is loopback, whatever's configured.
    if (ip.rfind("127.", 0) == 0) return true;
    auto all = localAddresses();
    return std::find(all.begin(), all.end(), ip) != all.end();
}

std::vector<std::string> interfacesSharingSubnet(const std::string & ifname) {
    in_addr ours{}, ourMask{};
    bool found = false;
    forEachIPv4([&](const std::string & name, in_addr addr, in_addr mask, bool up) {
        if (!found && up && name == ifname) {
            ours = addr;
            ourMask = mask;
            found = true;
        }
    });

    std::vector<std::string> result;
    if (!found) return result;

    uint32_t net = ours.s_addr & ourMask.s_addr;
    forEachIPv4([&](const std::string & name, in_addr addr, in_addr, bool up) {
        if (!up || name == ifname || name == "lo") return;
        if ((addr.s_addr & ourMask.s_addr) == net &&
            std::find(result.begin(), result.end(), name) == result.end()) {
            result.push_back(name);
        }
    });
    return result;
}

std::vector<std::string> resolve(const std::string & host) {
    std::vector<std::string> result;
    if (host.empty()) return result;

    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;

    addrinfo * info = nullptr;
    if (getaddrinfo(host.c_str(), nullptr, &hints, &info) != 0) return result;
    for (addrinfo * i = info; i; i = i->ai_next) {
        std::string ip = toString(((sockaddr_in *)i->ai_addr)->sin_addr);
        if (std::find(result.begin(), result.end(), ip) == result.end()) result.push_back(ip);
    }
    freeaddrinfo(info);
    return result;
}

} // namespace net
} // namespace ofxSyncing
