#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "ofxSyncingConfig.h"
#include "ofxSyncingTypes.h"

namespace ofxSyncing {

class Node;

// Finds candidate peers. Discovery is pluggable because its scope depends on
// the network: mDNS and multicast don't cross the internet.
//
// A backend must never hold its own lock while calling into the node.
class Discovery {
public:
    virtual ~Discovery() = default;

    virtual std::string getName() const = 0;

    // Node::start() and stop() call these.
    virtual bool start(Node & node) = 0;
    virtual void stop() = 0;

    // The node's BEACON, once per beacon interval, for backends that send it
    // themselves.
    virtual void announce(const Message & beacon) {}

    // Where the transport should look for peers right now.
    virtual std::vector<Endpoint> getEndpoints() const = 0;
};

// The backend config.discovery asks for. Bonjour isn't built into the core
// (it needs libavahi-client-dev), so for that this returns nullptr: include
// ofxSyncingAvahi.h and pass an AvahiDiscovery to Node::setup() instead.
std::unique_ptr<Discovery> makeDiscovery(const Config & config);

// For transports that find their peers themselves, like the WebSocket
// adapter: clients connect to one server, and the server hears from them.
class NoDiscovery: public Discovery {
public:
    std::string getName() const override { return "none"; }
    bool start(Node &) override { return true; }
    void stop() override {}
    std::vector<Endpoint> getEndpoints() const override { return {}; }
};

// Hostnames or IPs from settings.xml, for predefined topologies. Names are
// resolved with plain getaddrinfo on a thread of their own (a .local lookup
// can block for seconds) and re-resolved periodically.
class StaticDiscovery: public Discovery {
public:
    StaticDiscovery(const std::vector<std::string> & peers, int defaultPort, int64_t refreshUs = 30000000);
    ~StaticDiscovery();

    std::string getName() const override { return "static"; }
    bool start(Node & node) override;
    void stop() override;
    std::vector<Endpoint> getEndpoints() const override;

private:
    void run();

    std::vector<Endpoint> names;  // as configured, unresolved
    int64_t refreshUs;

    mutable std::mutex mutex;
    std::condition_variable cv;
    bool running = false;
    std::thread thread;
    std::vector<Endpoint> endpoints;
};

// The default: every node sends its BEACON to a multicast group once a
// beacon interval, and learns its peers from theirs. No dependencies.
// With an interface configured it sends, joins and listens on that one only.
class BeaconDiscovery: public Discovery {
public:
    BeaconDiscovery(const std::string & group, const std::string & multicastGroup, int multicastPort, const std::string & interface);
    ~BeaconDiscovery();

    std::string getName() const override { return "beacon"; }
    bool start(Node & node) override;
    void stop() override;
    void announce(const Message & beacon) override;
    std::vector<Endpoint> getEndpoints() const override;

private:
    bool open(bool report);
    void run();

    std::string group;
    std::string multicastGroup;
    int multicastPort;
    std::string interface;
    unsigned ifindex = 0;

    Node * node = nullptr;
    std::atomic<int> sock{-1};
    std::atomic<bool> running{false};
    std::thread thread;

    mutable std::mutex mutex;
    std::map<Endpoint, int64_t> seen;  // endpoint -> last heard, local µs
};

} // namespace ofxSyncing
