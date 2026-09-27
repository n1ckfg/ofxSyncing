#pragma once

#include <functional>
#include <map>
#include <memory>
#include <queue>
#include <random>
#include <set>
#include <tuple>
#include <string>
#include <vector>

#include "ofxSyncingClock.h"
#include "ofxSyncingConfig.h"
#include "ofxSyncingNode.h"
#include "ofxSyncingTransport.h"

// An in-process network on simulated time, with delay, jitter and loss.
// Hours of simulated sync run in seconds, and a seed makes every run the
// same. The unit tests are built on it.

namespace ofxSyncing {

class Simulation;

// A node's crystal: local = offset + true * (1 + drift).
class SimClock: public LocalClock {
public:
    SimClock(const Simulation & sim, int64_t offsetUs, double driftPpm);

    int64_t now() const override;
    int64_t fromTrue(int64_t trueUs) const;
    int64_t toTrue(int64_t localUs) const;  // rounds up

private:
    const Simulation & sim;
    int64_t offsetUs;
    double rate;
};

class LoopbackTransport: public Transport {
public:
    LoopbackTransport(Simulation & sim, const std::string & name);

    bool send(const Endpoint & to, const Message & msg) override;
    void broadcast(const Message & msg) override;
    bool isLocal(const Endpoint & e) const override { return e.host == name; }

    const std::string & getName() const { return name; }

private:
    friend class Simulation;
    Simulation & sim;
    std::string name;
};

class Simulation {
public:
    // One direction of a link. The delay is delayUs +/- jitterUs, uniform,
    // like netem's "delay 20ms 10ms". A spike adds up to spikeUs more, the
    // way WiFi does.
    struct Link {
        int64_t delayUs = 150;
        int64_t jitterUs = 50;
        double loss = 0.0;
        double spikeChance = 0.0;
        int64_t spikeUs = 0;
    };

    explicit Simulation(uint64_t seed = 1);
    ~Simulation();

    // A node with its own crystal, set up on the loopback transport (which
    // reaches every other node, so no discovery) and started unthreaded.
    Node & addNode(Config config, int64_t clockOffsetUs = 0, double driftPpm = 0.0);

    Node * node(const std::string & id);
    std::vector<Node *> nodes();
    SimClock * clockOf(const std::string & id);

    // Stops a node (it goes silent) and starts it again as a returning node.
    void kill(const std::string & id);
    void revive(const std::string & id);

    void setDefaultLink(const Link & link) { defaultLink = link; }
    void setLink(const std::string & from, const std::string & to, const Link & link);

    // Splits the network in two: one side is the given nodes, the other side
    // everyone else. Messages across are lost until heal().
    void partition(const std::set<std::string> & side);
    void heal();

    // True time, µs since the simulation began.
    int64_t now() const { return trueNow; }

    // Runs time forward. probe runs every probeIntervalUs, if given.
    void runFor(int64_t durationUs, const std::function<void()> & probe = nullptr, int64_t probeIntervalUs = 0);

    // A node's shared clock at the current true time.
    int64_t sharedTimeOf(const std::string & id);

    // The largest difference between any two running, locked nodes' shared
    // clocks, right now.
    int64_t spreadUs();

    uint64_t getMessagesSent() const { return sent; }
    uint64_t getMessagesLost() const { return lost; }

private:
    friend class LoopbackTransport;
    friend class SimClock;

    void enqueue(const std::string & from, const std::string & to, const Message & msg);
    void pollNode(const std::string & id);

    struct Member {
        std::shared_ptr<SimClock> clock;
        std::unique_ptr<LoopbackTransport> transport;
        std::unique_ptr<Node> node;
        int64_t wakeTrue = 0;
    };

    struct InFlight {
        int64_t at;
        uint64_t order;
        std::string from;
        std::string to;
        Message msg;
        bool operator>(const InFlight & o) const { return std::tie(at, order) > std::tie(o.at, o.order); }
    };

    std::map<std::string, Member> members;
    std::priority_queue<InFlight, std::vector<InFlight>, std::greater<InFlight>> inFlight;
    int64_t trueNow = 0;
    uint64_t order = 0;
    uint64_t sent = 0;
    uint64_t lost = 0;

    std::mt19937_64 rng;
    Link defaultLink;
    std::map<std::pair<std::string, std::string>, Link> links;
    bool partitioned = false;
    std::set<std::string> side;
};

} // namespace ofxSyncing
