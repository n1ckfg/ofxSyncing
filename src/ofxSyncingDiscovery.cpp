#include "ofxSyncingDiscovery.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "ofLog.h"

#include "ofxSyncingClock.h"
#include "ofxSyncingCodec.h"
#include "ofxSyncingNet.h"
#include "ofxSyncingNode.h"

namespace ofxSyncing {

std::unique_ptr<Discovery> makeDiscovery(const Config & config) {
    switch (config.discovery) {
        case DiscoveryMode::Static:
            return std::make_unique<StaticDiscovery>(config.peers, config.port);
        case DiscoveryMode::Beacon:
            return std::make_unique<BeaconDiscovery>(config.group, config.multicastGroup, config.multicastPort, config.interface);
        case DiscoveryMode::Bonjour:
            ofLogError("ofxSyncing") << "Bonjour discovery isn't built in: include ofxSyncingAvahi.h and pass "
                                     << "std::make_unique<ofxSyncing::AvahiDiscovery>() to setup()";
            return nullptr;
    }
    return nullptr;
}

// --- StaticDiscovery --------------------------------------------------------

StaticDiscovery::StaticDiscovery(const std::vector<std::string> & peers, int defaultPort, int64_t refreshUs):
    refreshUs(refreshUs) {
    for (auto & p : peers) {
        Endpoint e = Endpoint::parse(p, defaultPort);
        if (!e.empty()) names.push_back(e);
    }
}

StaticDiscovery::~StaticDiscovery() {
    stop();
}

bool StaticDiscovery::start(Node &) {
    std::lock_guard<std::mutex> lock(mutex);
    if (running) return true;
    running = true;
    thread = std::thread(&StaticDiscovery::run, this);
    return true;
}

void StaticDiscovery::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running) return;
        running = false;
    }
    cv.notify_all();
    if (thread.joinable()) thread.join();
}

std::vector<Endpoint> StaticDiscovery::getEndpoints() const {
    std::lock_guard<std::mutex> lock(mutex);
    return endpoints;
}

void StaticDiscovery::run() {
    std::map<std::string, bool> warned;

    while (true) {
        std::vector<Endpoint> resolved;
        for (auto & name : names) {
            auto ips = net::resolve(name.host);
            if (ips.empty()) {
                if (!warned[name.host]) {
                    ofLogWarning("ofxSyncing") << "can't resolve peer " << name.host << ", will keep trying";
                    warned[name.host] = true;
                }
                continue;
            }
            warned[name.host] = false;
            resolved.emplace_back(ips.front(), name.port);
        }

        std::unique_lock<std::mutex> lock(mutex);
        endpoints = resolved;

        // Retry soon while any name is missing, e.g. before the network is up.
        int64_t wait = resolved.size() < names.size() ? std::min<int64_t>(refreshUs, 2000000) : refreshUs;
        cv.wait_for(lock, std::chrono::microseconds(wait), [this] { return !running; });
        if (!running) return;
    }
}

// --- BeaconDiscovery --------------------------------------------------------

BeaconDiscovery::BeaconDiscovery(const std::string & group, const std::string & multicastGroup, int multicastPort, const std::string & interface):
    group(group), multicastGroup(multicastGroup), multicastPort(multicastPort), interface(interface) {
}

BeaconDiscovery::~BeaconDiscovery() {
    stop();
}

bool BeaconDiscovery::start(Node & n) {
    if (running) return true;
    node = &n;

    in_addr groupAddr{};
    if (inet_pton(AF_INET, multicastGroup.c_str(), &groupAddr) != 1) {
        ofLogError("ofxSyncing") << "bad multicast group " << multicastGroup;
        return false;
    }

    // The socket opens on the discovery thread, which keeps trying: an app
    // started at boot may come up before the network does.
    running = true;
    thread = std::thread(&BeaconDiscovery::run, this);
    return true;
}

bool BeaconDiscovery::open(bool report) {
    in_addr groupAddr{};
    inet_pton(AF_INET, multicastGroup.c_str(), &groupAddr);

    ifindex = net::interfaceIndex(interface);
    if (!interface.empty() && ifindex == 0) {
        if (report) ofLogWarning("ofxSyncing") << "no interface " << interface << " for multicast discovery yet";
        return false;
    }

    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0) {
        if (report) ofLogError("ofxSyncing") << "multicast socket: " << strerror(errno);
        return false;
    }

    // Every node on this machine binds the same port and gets a copy.
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif

    sockaddr_in bindAddr{};
    bindAddr.sin_family = AF_INET;
    bindAddr.sin_port = htons(multicastPort);
    bindAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(fd, (sockaddr *)&bindAddr, sizeof(bindAddr)) != 0) {
        if (report) ofLogError("ofxSyncing") << "multicast bind to port " << multicastPort << ": " << strerror(errno);
        close(fd);
        return false;
    }

    ip_mreqn mreq{};
    mreq.imr_multiaddr = groupAddr;
    mreq.imr_ifindex = (int)ifindex;
    if (setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) != 0) {
        if (report) {
            ofLogWarning("ofxSyncing") << "can't join " << multicastGroup << " yet (" << strerror(errno)
                                       << "); will keep trying";
        }
        close(fd);
        return false;
    }

#ifdef __linux__
    // Only this socket's own memberships, not every group on the machine.
    int zero = 0;
    setsockopt(fd, IPPROTO_IP, IP_MULTICAST_ALL, &zero, sizeof(zero));
    // Which interface each beacon arrived on.
    setsockopt(fd, IPPROTO_IP, IP_PKTINFO, &one, sizeof(one));
#endif

    if (ifindex) {
        ip_mreqn out{};
        out.imr_ifindex = (int)ifindex;
        setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &out, sizeof(out));
    }

    unsigned char ttl = 1;
    setsockopt(fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));
    unsigned char loop = 1;
    setsockopt(fd, IPPROTO_IP, IP_MULTICAST_LOOP, &loop, sizeof(loop));

    sock = fd;
    ofLogNotice("ofxSyncing") << "multicast discovery on " << multicastGroup << ":" << multicastPort
                              << (interface.empty() ? "" : " (" + interface + ")");
    return true;
}

void BeaconDiscovery::stop() {
    if (!running) return;
    running = false;
    if (thread.joinable()) thread.join();
    int fd = sock.exchange(-1);
    if (fd >= 0) close(fd);
}

void BeaconDiscovery::announce(const Message & beacon) {
    int fd = sock;
    if (fd < 0) return;

    std::string text = encodeJson(beacon);

    sockaddr_in to{};
    to.sin_family = AF_INET;
    to.sin_port = htons(multicastPort);
    inet_pton(AF_INET, multicastGroup.c_str(), &to.sin_addr);
    sendto(fd, text.data(), text.size(), 0, (sockaddr *)&to, sizeof(to));
}

std::vector<Endpoint> BeaconDiscovery::getEndpoints() const {
    std::vector<Endpoint> result;
    int64_t now = node ? node->getLocalTimeUs() : 0;
    std::lock_guard<std::mutex> lock(mutex);
    for (auto & s : seen) {
        // Forget peers that stopped beaconing long ago.
        if (now - s.second < 30000000) result.push_back(s.first);
    }
    return result;
}

void BeaconDiscovery::run() {
    std::vector<char> buffer(65536);
    bool report = true;
    int64_t retryAt = 0;

    while (running) {
        if (sock < 0) {
            int64_t now = monotonicUs();
            if (now >= retryAt) {
                if (!open(report)) {
                    report = false;
                    retryAt = now + 2000000;
                }
            }
            if (sock < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(200));
                continue;
            }
        }

        pollfd pfd{};
        pfd.fd = sock;
        pfd.events = POLLIN;
        if (::poll(&pfd, 1, 200) <= 0) continue;

        sockaddr_in from{};
        iovec iov{buffer.data(), buffer.size()};
        char control[256];
        msghdr msg{};
        msg.msg_name = &from;
        msg.msg_namelen = sizeof(from);
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        msg.msg_control = control;
        msg.msg_controllen = sizeof(control);

        ssize_t n = recvmsg(sock, &msg, 0);
        if (n <= 0) continue;
        int64_t rx = node->getLocalTimeUs();

#ifdef __linux__
        if (ifindex) {
            unsigned arrivedOn = 0;
            for (cmsghdr * c = CMSG_FIRSTHDR(&msg); c; c = CMSG_NXTHDR(&msg, c)) {
                if (c->cmsg_level == IPPROTO_IP && c->cmsg_type == IP_PKTINFO) {
                    arrivedOn = (unsigned)((in_pktinfo *)CMSG_DATA(c))->ipi_ifindex;
                }
            }
            if (arrivedOn && arrivedOn != ifindex) continue;
        }
#endif

        Message beacon;
        if (!decodeJson(std::string(buffer.data(), (size_t)n), beacon)) continue;
        if (beacon.type != MessageType::Beacon || beacon.group != group) continue;

        char ip[INET_ADDRSTRLEN] = {0};
        inet_ntop(AF_INET, &from.sin_addr, ip, sizeof(ip));
        std::string fromHost = ip;

        if (beacon.nodeId != node->getNodeId() && beacon.port > 0) {
            Endpoint e(beacon.address.empty() ? fromHost : beacon.address, beacon.port);
            std::lock_guard<std::mutex> lock(mutex);
            seen[e] = rx;
        }

        node->receiveDiscovery(beacon, fromHost, rx);
    }
}

} // namespace ofxSyncing
