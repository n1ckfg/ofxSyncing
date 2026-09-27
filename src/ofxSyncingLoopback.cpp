#include "ofxSyncingLoopback.h"

#include "ofxSyncingDiscovery.h"

#include <algorithm>
#include <cmath>

namespace ofxSyncing {

// --- SimClock ----------------------------------------------------------------

SimClock::SimClock(const Simulation & sim, int64_t offsetUs, double driftPpm):
    sim(sim), offsetUs(offsetUs), rate(1.0 + driftPpm * 1e-6) {
}

int64_t SimClock::now() const {
    return fromTrue(sim.trueNow);
}

int64_t SimClock::fromTrue(int64_t trueUs) const {
    return offsetUs + (int64_t)std::floor((double)trueUs * rate);
}

int64_t SimClock::toTrue(int64_t localUs) const {
    int64_t t = (int64_t)std::ceil((double)(localUs - offsetUs) / rate);
    while (fromTrue(t) < localUs) t++;
    return t;
}

// --- LoopbackTransport -------------------------------------------------------

LoopbackTransport::LoopbackTransport(Simulation & sim, const std::string & name):
    sim(sim), name(name) {
}

bool LoopbackTransport::send(const Endpoint & to, const Message & msg) {
    sim.enqueue(name, to.host, msg);
    return true;
}

void LoopbackTransport::broadcast(const Message & msg) {
    for (auto & m : sim.members) {
        if (m.first != name) sim.enqueue(name, m.first, msg);
    }
}

// --- Simulation --------------------------------------------------------------

Simulation::Simulation(uint64_t seed):
    rng(seed) {
}

Simulation::~Simulation() {
    // Nodes go before the transports they're attached to.
    for (auto & m : members) m.second.node.reset();
}

Node & Simulation::addNode(Config config, int64_t clockOffsetUs, double driftPpm) {
    std::string id = config.resolvedNodeId();
    config.nodeId = id;

    Member & m = members[id];
    m.clock = std::make_shared<SimClock>(*this, clockOffsetUs, driftPpm);
    m.transport = std::make_unique<LoopbackTransport>(*this, id);
    m.node = std::make_unique<Node>();
    m.node->setLocalClock(m.clock);

    // Everyone is reachable, so no discovery is needed.
    m.node->setup(config, *m.transport, std::make_unique<NoDiscovery>());
    m.node->start(false);
    m.wakeTrue = trueNow;
    return *m.node;
}

Node * Simulation::node(const std::string & id) {
    auto it = members.find(id);
    return it == members.end() ? nullptr : it->second.node.get();
}

std::vector<Node *> Simulation::nodes() {
    std::vector<Node *> result;
    for (auto & m : members) result.push_back(m.second.node.get());
    return result;
}

SimClock * Simulation::clockOf(const std::string & id) {
    auto it = members.find(id);
    return it == members.end() ? nullptr : it->second.clock.get();
}

void Simulation::kill(const std::string & id) {
    if (auto n = node(id)) n->stop();
}

void Simulation::revive(const std::string & id) {
    auto it = members.find(id);
    if (it == members.end()) return;
    it->second.node->start(false);
    it->second.wakeTrue = trueNow;
}

void Simulation::setLink(const std::string & from, const std::string & to, const Link & link) {
    links[{from, to}] = link;
}

void Simulation::partition(const std::set<std::string> & s) {
    side = s;
    partitioned = true;
}

void Simulation::heal() {
    partitioned = false;
    side.clear();
}

void Simulation::enqueue(const std::string & from, const std::string & to, const Message & msg) {
    if (members.find(to) == members.end()) return;
    sent++;

    if (partitioned && side.count(from) != side.count(to)) {
        lost++;
        return;
    }

    auto it = links.find({from, to});
    const Link & link = it == links.end() ? defaultLink : it->second;

    std::uniform_real_distribution<double> unit(0.0, 1.0);
    if (link.loss > 0.0 && unit(rng) < link.loss) {
        lost++;
        return;
    }

    double delay = (double)link.delayUs;
    if (link.jitterUs > 0) delay += (unit(rng) * 2.0 - 1.0) * (double)link.jitterUs;
    if (link.spikeChance > 0.0 && unit(rng) < link.spikeChance) delay += unit(rng) * (double)link.spikeUs;
    int64_t d = std::max<int64_t>(1, (int64_t)std::llround(delay));

    inFlight.push(InFlight{trueNow + d, order++, from, to, msg});
}

void Simulation::pollNode(const std::string & id) {
    Member & m = members[id];
    int64_t next = m.node->poll(m.clock->fromTrue(trueNow));
    m.wakeTrue = next == NEVER ? NEVER : std::max(trueNow + 1, m.clock->toTrue(next));
}

void Simulation::runFor(int64_t durationUs, const std::function<void()> & probe, int64_t probeIntervalUs) {
    int64_t end = trueNow + durationUs;
    int64_t nextProbe = probe && probeIntervalUs > 0 ? trueNow : NEVER;

    // Anything the test did between runs (scheduling an event, say) may
    // have changed when a node wants to wake.
    for (auto & m : members) m.second.wakeTrue = trueNow;

    while (true) {
        int64_t t = NEVER;
        for (auto & m : members) t = std::min(t, m.second.wakeTrue);
        if (!inFlight.empty()) t = std::min(t, inFlight.top().at);
        t = std::min(t, nextProbe);
        if (t > end) break;
        trueNow = std::max(trueNow, t);

        std::set<std::string> touched;
        while (!inFlight.empty() && inFlight.top().at <= trueNow) {
            InFlight f = inFlight.top();
            inFlight.pop();
            Member & m = members[f.to];
            m.node->receive(f.msg, Endpoint(f.from, 1), m.clock->fromTrue(trueNow));
            touched.insert(f.to);
        }

        for (auto & m : members) {
            if (m.second.wakeTrue <= trueNow || touched.count(m.first)) pollNode(m.first);
        }

        if (trueNow >= nextProbe) {
            probe();
            nextProbe += probeIntervalUs;
        }
    }
    trueNow = end;
}

int64_t Simulation::sharedTimeOf(const std::string & id) {
    auto it = members.find(id);
    if (it == members.end()) return 0;
    return it->second.node->sharedFromLocal(it->second.clock->fromTrue(trueNow));
}

int64_t Simulation::spreadUs() {
    int64_t lo = NEVER, hi = -NEVER;
    for (auto & m : members) {
        Node & n = *m.second.node;
        if (!n.isRunning() || !n.isLocked()) continue;
        int64_t s = n.sharedFromLocal(m.second.clock->fromTrue(trueNow));
        lo = std::min(lo, s);
        hi = std::max(hi, s);
    }
    return lo == NEVER ? 0 : hi - lo;
}

} // namespace ofxSyncing
