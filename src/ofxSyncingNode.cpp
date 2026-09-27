#include "ofxSyncingNode.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstring>
#include <random>
#include <sstream>
#include <tuple>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

#include "ofLog.h"
#include "ofUtils.h"

#include "ofxSyncingDiscovery.h"
#include "ofxSyncingNet.h"
#include "ofxSyncingTransport.h"

namespace ofxSyncing {

namespace {

const char * LOG = "ofxSyncing";

// Initial lock: a burst of pings about 50 ms apart.
constexpr int BURST_COUNT = 8;
constexpr int64_t BURST_SPACING_US = 50000;
constexpr int64_t BURST_WAIT_US = 1000000;        // for replies after the last one

constexpr int64_t REQUEST_RESEND_US = 100000;
constexpr int64_t REQUEST_GIVE_UP_US = 10000000;
constexpr int64_t MIN_RESEND_US = 20000;          // events, to a follower that hasn't acked
constexpr int64_t SNAPSHOT_INTERVAL_US = 250000;  // at most this often per follower

constexpr int64_t DONE_RETENTION_US = 120000000;  // how long a fired event's id is remembered
constexpr int64_t MAX_EVENT_AGE_US = 60000000;    // an event this far in the past never fires
constexpr int64_t PING_RETENTION_US = 10000000;
constexpr int64_t REQUEST_RETENTION_US = 60000000;
constexpr int64_t PEER_RETENTION_US = 300000000;
constexpr int64_t COARSE_MAX_AGE_US = 10000000;

constexpr size_t MAX_MAIN_QUEUE = 1024;
constexpr size_t MAX_NOTIFICATIONS = 256;

// The scheduler waits on a condition variable until this close to a
// deadline, then clock_nanosleeps the rest of the way.
constexpr int64_t FINE_SLEEP_US = 2000;
constexpr int64_t MAX_SCHEDULER_WAIT_US = 100000;
constexpr int64_t MAX_SERVICE_WAIT_US = 1000000;

// Term first, then priority, then node id.
bool outranks(int64_t termA, int priorityA, const std::string & idA,
              int64_t termB, int priorityB, const std::string & idB) {
    return std::make_tuple(termA, priorityA, idA) > std::make_tuple(termB, priorityB, idB);
}

std::string ms(int64_t us) {
    return ofToString((double)us / 1000.0, 3) + " ms";
}

} // namespace

const char * toString(Role role) {
    switch (role) {
        case Role::Stopped: return "stopped";
        case Role::Starting: return "starting";
        case Role::Follower: return "follower";
        case Role::Master: return "master";
    }
    return "unknown";
}

const char * toString(Discontinuity::Reason reason) {
    return reason == Discontinuity::Reason::MasterConflict ? "master conflict" : "clock step";
}

Node::Node():
    localClock(std::make_shared<LocalClock>()) {
    // Event numbers start at random, so a restarted node doesn't reuse ids
    // the others still remember. 48 bits stay exact in a browser.
    std::random_device rd;
    eventSeq = ((((uint64_t)rd()) << 32) | (uint64_t)rd()) & ((1ull << 48) - 1);
}

Node::~Node() {
    stop();
    if (transport) transport->attach(nullptr);
}

void Node::setup(const Config & cfg, Transport & t, std::unique_ptr<Discovery> d) {
    stop();

    std::lock_guard<std::mutex> lock(mutex);
    config = cfg;
    if (config.pingHz <= 0.0) config.pingHz = 1.0;
    if (config.beaconIntervalUs <= 0) config.beaconIntervalUs = 1000000;
    if (config.masterTimeoutUs <= config.beaconIntervalUs) config.masterTimeoutUs = 3 * config.beaconIntervalUs;
    nodeId = config.resolvedNodeId();

    if (transport && transport != &t) transport->attach(nullptr);
    transport = &t;
    transport->attach(this);

    discovery = d ? std::move(d) : makeDiscovery(config);

    clock.setSlewRate(config.slewRateUsPerSec);
    clock.setStepThreshold(config.stepThresholdUs);

    announcedAddress.clear();
    if (!config.interface.empty()) {
        announcedAddress = net::interfaceAddress(config.interface);
        if (announcedAddress.empty()) {
            ofLogWarning(LOG) << "interface " << config.interface << " has no IPv4 address yet";
        }
        auto sharing = net::interfacesSharingSubnet(config.interface);
        if (!sharing.empty()) {
            ofLogWarning(LOG) << ofJoinString(sharing, ", ") << " on the same subnet as " << config.interface
                              << ": traffic can leave through either, so sync may run over the wrong one."
                              << " See the README's Raspberry Pi notes";
        }
    }

    if (csv.is_open()) csv.close();
    if (!config.csvLog.empty()) {
        std::string path = ofToDataPath(config.csvLog, true);
        csv.open(path, std::ios::out | std::ios::app);
        if (!csv) {
            ofLogWarning(LOG) << "couldn't open " << path << " for the CSV log";
        } else if (csv.tellp() == 0) {
            csv << "local_us,shared_us,kind,node,master,term,offset_us,delay_us,error_us,drift_ppm,locked\n";
        }
    }
}

void Node::setLocalClock(std::shared_ptr<LocalClock> c) {
    std::lock_guard<std::mutex> lock(mutex);
    localClock = c ? c : std::make_shared<LocalClock>();
}

bool Node::start(bool threaded) {
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!transport) {
            ofLogError(LOG) << "start() before setup()";
            return false;
        }
        if (running) return true;

        int64_t now = localClock->now();
        running = true;
        startedAt = now;
        role = Role::Starting;
        masterId.clear();
        syncPhase = SyncPhase::Idle;
        burstSamples.clear();
        outstandingPings.clear();
        haveSnapshot = false;
        nextBeaconAt = now;
        electionSince = now;
        skippedCandidates.clear();

        // A returning node's view of the others is stale, and events it
        // missed come back in the master's snapshot.
        for (auto & p : peers) p.second.heard = false;
        pending.clear();
        eventRequests.clear();
        timelineRequests.clear();
        timelineResends.clear();

        noteRoleLocked();
        ofLogNotice(LOG) << nodeId << " starting: group " << config.group << ", priority " << config.priority
                         << ", discovery " << (discovery ? discovery->getName() : "none");
    }

    if (discovery && !discovery->start(*this)) {
        ofLogWarning(LOG) << discovery->getName() << " discovery didn't start";
    }

    if (threaded) {
        threadsRunning = true;
        serviceThread = std::thread(&Node::serviceLoop, this);
        schedulerThread = std::thread(&Node::schedulerLoop, this);
    }
    return true;
}

void Node::stop() {
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (running) {
            running = false;
            role = Role::Stopped;
            masterId.clear();
            noteRoleLocked();
            ofLogNotice(LOG) << nodeId << " stopped";
        }
    }
    stopThreads();
    if (discovery) discovery->stop();
}

void Node::stopThreads() {
    threadsRunning = false;
    kickService();
    kickScheduler();
    if (serviceThread.joinable()) serviceThread.join();
    if (schedulerThread.joinable()) schedulerThread.join();
}

bool Node::isRunning() const {
    std::lock_guard<std::mutex> lock(mutex);
    return running;
}

// --- receiving ---------------------------------------------------------------

void Node::receive(const Message & msg, const Endpoint & from, int64_t rx) {
    std::vector<Outgoing> out;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running || msg.group != config.group || msg.nodeId.empty()) return;
        if (msg.nodeId == nodeId) {
            // Our own broadcast came back: never send to that endpoint again.
            if (!from.empty()) selfEndpoints.insert(from);
            return;
        }

        Peer & peer = heardFromLocked(msg, rx);
        if (!from.empty()) {
            peer.route = from;
            peer.routeFromTransport = true;
        }
        handleLocked(msg, from, rx, peer);
        out.swap(outbox);
    }
    flush(out);
}

void Node::receiveDiscovery(const Message & beacon, const std::string & fromHost, int64_t rx) {
    if (beacon.type != MessageType::Beacon) return;

    std::vector<Outgoing> out;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running || beacon.group != config.group || beacon.nodeId.empty() || beacon.nodeId == nodeId) return;

        Peer & peer = heardFromLocked(beacon, rx);
        if (!peer.routeFromTransport && beacon.port > 0) {
            peer.route = Endpoint(beacon.address.empty() ? fromHost : beacon.address, beacon.port);
        }
        handleLocked(beacon, peer.route, rx, peer);
        out.swap(outbox);
    }
    flush(out);
}

Node::Peer & Node::heardFromLocked(const Message & msg, int64_t now) {
    Peer & p = peers[msg.nodeId];
    if (p.nodeId.empty()) {
        p.nodeId = msg.nodeId;
        ofLogNotice(LOG) << "found peer " << msg.nodeId;
    }
    p.heard = true;
    p.lastHeard = std::max(p.lastHeard, now);
    maxTermSeen = std::max(maxTermSeen, msg.term);

    if (msg.type == MessageType::Beacon) {
        p.priority = msg.priority;
        p.isMaster = msg.isMaster;
        p.masterId = msg.masterId;
        p.term = msg.term;
        p.stats = msg.stats;
        p.leadUs = msg.leadUs;
        if (msg.stats.locked && msg.sharedUs != 0) {
            coarseValid = true;
            coarseSharedUs = msg.sharedUs;
            coarseLocalUs = now;
        }
        logCsvLocked(now, "peer", msg.nodeId, msg.masterId, msg.term, 0, msg.stats);
    }
    return p;
}

Node::Peer * Node::findPeerLocked(const std::string & id) {
    auto it = peers.find(id);
    return it == peers.end() ? nullptr : &it->second;
}

const Node::Peer * Node::findPeerLocked(const std::string & id) const {
    auto it = peers.find(id);
    return it == peers.end() ? nullptr : &it->second;
}

bool Node::isLiveLocked(const Peer & peer, int64_t now) const {
    return peer.heard && now - peer.lastHeard < config.masterTimeoutUs;
}

void Node::handleLocked(const Message & msg, const Endpoint & from, int64_t now, Peer & peer) {
    switch (msg.type) {
        case MessageType::Beacon:
            if (msg.isMaster) {
                considerMasterLocked(msg.nodeId, msg.term, msg.priority, now);
            } else if (role == Role::Follower && msg.nodeId == masterId && msg.term >= term) {
                // Our master stepped down, most likely after losing a merge.
                // Follow whoever it follows now if we know them as a master.
                // (A beacon from before it became master, overtaken on the
                // way, has an older term.)
                const Peer * next = findPeerLocked(msg.masterId);
                if (next && next->isMaster && isLiveLocked(*next, now)) {
                    considerMasterLocked(next->nodeId, next->term, next->priority, now);
                }
                if (masterId == msg.nodeId) masterLostLocked(now);
            }
            break;

        case MessageType::Ping: {
            if (role != Role::Master) break;
            Message pong = makeLocked(MessageType::Pong);
            pong.seq = msg.seq;
            pong.t0 = msg.t0;
            pong.t1 = clock.toShared(now);
            queueSendLocked(from, pong);  // t2 is stamped right before it goes

            if (msg.wantSnapshot && (!peer.snapshotSent || now - peer.lastSnapshotSent >= SNAPSHOT_INTERVAL_US)) {
                queueSendLocked(from, snapshotLocked(now));
                peer.snapshotSent = true;
                peer.lastSnapshotSent = now;
            }
            break;
        }

        case MessageType::Pong:
            if (acceptFromMasterLocked(msg, peer, now)) onPongLocked(msg, now);
            break;

        case MessageType::Event: {
            if (!acceptFromMasterLocked(msg, peer, now)) break;
            // Ack every copy: the ack itself may have been lost.
            Message ack = makeLocked(MessageType::Ack);
            ack.event.id = msg.event.id;
            queueSendLocked(from, ack);
            eventRequests.erase(msg.event.id);
            addEventLocked(msg.event, now);
            break;
        }

        case MessageType::Ack:
            if (role == Role::Master) {
                auto it = pending.find(msg.event.id);
                if (it != pending.end()) it->second.acks.insert(msg.nodeId);
            }
            break;

        case MessageType::EventRequest: {
            if (role != Role::Master) break;
            auto it = pending.find(msg.event.id);
            if (it != pending.end()) {
                // Already scheduled; the requester missed the EVENT.
                queueSendLocked(from, eventMessageLocked(it->second.event));
                break;
            }
            if (done.count(msg.event.id)) break;
            Event ev = msg.event;
            ev.time = assignTimeLocked(msg.event.time, msg.leadUs, now);
            masterScheduleLocked(ev, now);
            break;
        }

        case MessageType::Timeline:
            if (msg.timeline.version == 0) {
                // A request.
                if (role != Role::Master) break;
                if (!msg.requestId.empty() && handledTimelineRequests.count(msg.requestId)) {
                    if (auto st = timelines.latest(msg.timeline.id)) {
                        queueSendLocked(from, timelineMessageLocked(*st, msg.requestId));
                    }
                    break;
                }
                applyTimelineChangeLocked(msg.timeline.id, msg.hasPosition, msg.timeline.p0, msg.hasRate,
                                          msg.timeline.rate, msg.leadUs, msg.timeline.t0, msg.requestId, now);
                break;
            }
            if (!acceptFromMasterLocked(msg, peer, now)) break;
            if (!msg.requestId.empty()) timelineRequests.erase(msg.requestId);
            timelines.apply(msg.timeline, clock.toShared(now));
            break;

        case MessageType::Snapshot:
            if (!acceptFromMasterLocked(msg, peer, now)) break;
            timelines.replaceAll(msg.timelines);
            for (auto & ev : msg.events) {
                Message ack = makeLocked(MessageType::Ack);
                ack.event.id = ev.id;
                queueSendLocked(from, ack);
                eventRequests.erase(ev.id);
                addEventLocked(ev, now);
            }
            haveSnapshot = true;
            break;
    }
}

// --- roles -------------------------------------------------------------------

void Node::considerMasterLocked(const std::string & sender, int64_t senderTerm, int senderPriority, int64_t now) {
    if (sender == nodeId || role == Role::Stopped) return;
    maxTermSeen = std::max(maxTermSeen, senderTerm);

    if (role == Role::Master) {
        // Two masters: a network split healed. The higher term wins, then
        // the higher priority.
        if (outranks(senderTerm, senderPriority, sender, term, config.priority, nodeId)) {
            ofLogWarning(LOG) << sender << " (term " << senderTerm << ") outranks this master; stepping down";
            followLocked(sender, senderTerm, now, true);
        }
        return;
    }

    // A returning node follows whoever is master, even if it remembers a
    // later term (say, from electing itself while cut off).
    if (role == Role::Starting) {
        term = senderTerm;
        followLocked(sender, senderTerm, now, false);
        return;
    }

    if (senderTerm < term) return;  // stale

    if (masterId.empty()) {
        followLocked(sender, senderTerm, now, false);
        return;
    }

    if (sender == masterId) {
        term = std::max(term, senderTerm);
        return;
    }

    const Peer * current = findPeerLocked(masterId);
    int currentPriority = current ? current->priority : 0;
    if (outranks(senderTerm, senderPriority, sender, term, currentPriority, masterId)) {
        // If our master is still talking, this is two masters meeting rather
        // than a failover.
        bool conflict = current && current->heard && now - current->lastHeard < 2 * config.beaconIntervalUs;
        followLocked(sender, senderTerm, now, conflict);
    }
}

bool Node::acceptFromMasterLocked(const Message & msg, const Peer & peer, int64_t now) {
    considerMasterLocked(msg.nodeId, msg.term, peer.priority, now);
    return role == Role::Follower && masterId == msg.nodeId && term == msg.term;
}

void Node::followLocked(const std::string & master, int64_t masterTerm, int64_t now, bool conflict) {
    role = Role::Follower;
    masterId = master;
    term = masterTerm;
    skippedCandidates.clear();

    // The new master continues the old one's rate, so the drift estimate
    // still holds; the offset samples don't.
    estimator.reset(true);
    startBurstLocked(now);
    haveSnapshot = false;
    timelineResends.clear();
    for (auto & p : pending) p.second.tracked = false;

    ofLogNotice(LOG) << nodeId << " following " << master << " (term " << term << ")";
    noteRoleLocked();
    if (conflict && everLocked) noteDiscontinuityLocked(Discontinuity::Reason::MasterConflict, 0);
}

void Node::masterLostLocked(int64_t now) {
    const Peer * m = findPeerLocked(masterId);
    ofLogWarning(LOG) << "lost master " << masterId
                      << (m ? " (silent for " + ms(now - m->lastHeard) + ")" : std::string());
    masterId.clear();
    syncPhase = SyncPhase::Idle;
    outstandingPings.clear();
    electionSince = now;
    noteRoleLocked();
    electLocked(now);
}

void Node::electLocked(int64_t now) {
    if (role != Role::Follower || !masterId.empty()) return;

    // The live node with the highest priority, ties broken by node id.
    std::string best;
    int bestPriority = 0;
    if (config.priority > 0) {
        best = nodeId;
        bestPriority = config.priority;
    }
    for (auto & kv : peers) {
        const Peer & p = kv.second;
        if (p.priority <= 0 || !isLiveLocked(p, now) || skippedCandidates.count(p.nodeId)) continue;
        if (best.empty() || std::make_tuple(p.priority, p.nodeId) > std::make_tuple(bestPriority, best)) {
            best = p.nodeId;
            bestPriority = p.priority;
        }
    }
    if (best.empty()) return;

    if (best == nodeId) {
        promoteLocked(now);
        return;
    }

    // The candidate should claim within a beacon or two. One that never does
    // can't hear us, or follows a master we can't hear: try the next.
    if (now - electionSince > 2 * config.masterTimeoutUs) {
        ofLogWarning(LOG) << best << " hasn't taken over as master; passing over it";
        skippedCandidates.insert(best);
        electionSince = now;
    }
}

void Node::promoteLocked(int64_t now) {
    role = Role::Master;
    masterId = nodeId;
    term = std::max(term, maxTermSeen) + 1;
    maxTermSeen = term;
    skippedCandidates.clear();
    syncPhase = SyncPhase::Idle;
    outstandingPings.clear();

    // Continue from this node's current shared time so timelines don't jump.
    // A node that never locked starts from a locked peer's beacon if it heard
    // one, which is good to about a one-way delay.
    if (!clock.isLocked()) {
        int64_t offset = 0;
        if (coarseValid && now - coarseLocalUs < COARSE_MAX_AGE_US) {
            offset = coarseSharedUs - coarseLocalUs;
            ofLogNotice(LOG) << "never locked; starting from a peer's shared time";
        }
        clock.steer(now, offset, 0.0);
    }
    everLocked = true;

    queueBeaconLocked(now);
    nextBeaconAt = now + config.beaconIntervalUs;

    // Take over whatever is still pending: nobody has acked this master yet.
    int64_t sharedNow = clock.toShared(now);
    for (auto & kv : pending) {
        Pending & p = kv.second;
        if (p.event.time <= sharedNow) continue;
        p.tracked = true;
        p.acks.clear();
        p.lastSent.clear();
        p.broadcastAt = now;
        queueBroadcastLocked(eventMessageLocked(p.event));
    }
    for (auto & st : timelines.all()) {
        queueBroadcastLocked(timelineMessageLocked(st, EventId()));
    }

    ofLogNotice(LOG) << nodeId << " is master (term " << term << ")";
    noteRoleLocked();
    kickScheduler();
}

// --- clock sync --------------------------------------------------------------

int64_t Node::pingIntervalUs() const {
    return (int64_t)std::llround(1e6 / config.pingHz);
}

void Node::syncServiceLocked(int64_t now, int64_t & next) {
    if (syncPhase == SyncPhase::Idle) startBurstLocked(now);

    if (syncPhase == SyncPhase::Burst) {
        if (burstSent < BURST_COUNT && now >= nextPingAt) {
            sendPingLocked(now);
            burstSent++;
            nextPingAt = now + BURST_SPACING_US;
            if (burstSent == BURST_COUNT) burstDeadline = now + BURST_WAIT_US;
        }
        if (burstSent >= BURST_COUNT && now >= burstDeadline) {
            finishBurstLocked(now);
        }
    }

    if (syncPhase == SyncPhase::Tracking && now >= nextPingAt) {
        sendPingLocked(now);
        nextPingAt = now + pingIntervalUs();
    }

    if (syncPhase == SyncPhase::Burst && burstSent >= BURST_COUNT) {
        next = std::min(next, burstDeadline);
    } else {
        next = std::min(next, nextPingAt);
    }
}

void Node::startBurstLocked(int64_t now) {
    syncPhase = SyncPhase::Burst;
    burstSent = 0;
    burstSamples.clear();
    burstDeadline = NEVER;
    nextPingAt = now;
    outstandingPings.clear();
    kickService();
}

void Node::sendPingLocked(int64_t now) {
    const Peer * m = findPeerLocked(masterId);
    if (!m || m->route.empty()) return;

    Message ping = makeLocked(MessageType::Ping);
    ping.seq = ++pingSeq;
    ping.wantSnapshot = !haveSnapshot;
    outstandingPings[ping.seq] = now;
    queueSendLocked(m->route, ping);  // t0 is stamped right before it goes
}

void Node::onPongLocked(const Message & msg, int64_t now) {
    auto it = outstandingPings.find(msg.seq);
    if (it == outstandingPings.end()) return;
    outstandingPings.erase(it);

    ClockSample s;
    s.t0 = msg.t0;
    s.t1 = msg.t1;
    s.t2 = msg.t2;
    s.t3 = now;
    if (s.t3 < s.t0 || s.t2 < s.t1) return;

    if (syncPhase == SyncPhase::Burst) {
        burstSamples.push_back(s);
        estimator.addSample(s);
        if ((int)burstSamples.size() >= BURST_COUNT) finishBurstLocked(now);
    } else if (syncPhase == SyncPhase::Tracking) {
        estimator.addSample(s);
        trackLocked(now);
    }

    if (csv.is_open()) {
        SyncStats stats = ownStatsLocked(now);
        stats.delayUs = s.delay();
        logCsvLocked(now, "self", nodeId, masterId, term, clock.toShared(now) - now, stats);
    }
}

void Node::finishBurstLocked(int64_t now) {
    if (burstSamples.size() < 2) {
        ofLogVerbose(LOG) << burstSamples.size() << " of " << BURST_COUNT << " burst pings answered; trying again";
        startBurstLocked(now);
        return;
    }

    // The lowest-delay quarter, median offset.
    auto est = estimator.burstEstimate(burstSamples, now);
    bool wasLocked = clock.isLocked();
    auto adj = clock.steer(now, est.offsetUs, estimator.getSkew());
    lastEstimate = est;
    burstSamples.clear();
    syncPhase = SyncPhase::Tracking;
    nextPingAt = now + pingIntervalUs();

    if (!wasLocked) {
        everLocked = true;
        ofLogNotice(LOG) << nodeId << " locked to " << masterId << ", round trip " << ms(est.minDelayUs);
    } else if (adj.stepped) {
        ofLogWarning(LOG) << "shared time stepped by " << ms(adj.stepUs);
        noteDiscontinuityLocked(Discontinuity::Reason::ClockStep, adj.stepUs);
    }
    kickScheduler();
}

void Node::trackLocked(int64_t now) {
    auto est = estimator.estimate(now);
    if (!est.valid) return;
    lastEstimate = est;

    int64_t current = clock.toShared(now) - now;
    if (std::llabs(est.offsetUs - current) > config.stepThresholdUs) {
        // Re-lock from a fresh burst, and step if it agrees.
        ofLogWarning(LOG) << "off by " << ms(est.offsetUs - current) << ", past the step threshold; re-locking";
        estimator.reset(true);
        startBurstLocked(now);
        return;
    }
    clock.steer(now, est.offsetUs, est.skew);
}

SyncStats Node::ownStatsLocked(int64_t now) const {
    SyncStats s;
    s.locked = clock.isLocked();
    if (role == Role::Master || !s.locked) return s;

    // Bounded by half the minimum delay (plus the best sample's age times
    // the drift uncertainty), and whatever is still being slewed in.
    s.errorUs = lastEstimate.errorUs + std::llabs(clock.remainingSlewUs(now));
    s.delayUs = lastEstimate.medianDelayUs;
    s.delayP99Us = lastEstimate.p99DelayUs;
    s.driftPpm = clock.getSkew() * 1e6;
    return s;
}

// --- events ------------------------------------------------------------------

bool Node::addEventLocked(const Event & ev, int64_t now) {
    if (ev.id.empty()) return false;

    // De-duplicate by id, so a resend or a clock step never fires twice.
    if (pending.count(ev.id) || done.count(ev.id)) return false;

    if (clock.isLocked() && clock.toShared(now) - ev.time > MAX_EVENT_AGE_US) {
        done[ev.id] = now;
        ofLogWarning(LOG) << "ignoring event " << ev.name << " from over a minute ago";
        return false;
    }

    Pending p;
    p.event = ev;
    pending.emplace(ev.id, std::move(p));
    kickScheduler();
    return true;
}

void Node::masterScheduleLocked(Event ev, int64_t now) {
    if (!addEventLocked(ev, now)) return;
    Pending & p = pending[ev.id];
    p.tracked = true;
    p.broadcastAt = now;
    queueBroadcastLocked(eventMessageLocked(ev));
}

int64_t Node::assignTimeLocked(int64_t requested, int64_t leadUs, int64_t now) const {
    int64_t sharedNow = clock.toShared(now);
    if (requested > 0) return std::max(requested, sharedNow);
    return sharedNow + std::max(leadTimeLocked(now), leadUs);
}

int64_t Node::leadTimeLocked(int64_t now) const {
    int64_t worst = 0;
    if (role == Role::Master) {
        for (auto & kv : peers) {
            if (isLiveLocked(kv.second, now)) worst = std::max(worst, kv.second.stats.delayP99Us);
        }
    } else {
        const Peer * m = findPeerLocked(masterId);
        if (m && m->leadUs > 0) return m->leadUs;
        worst = lastEstimate.p99DelayUs;
    }
    return std::max(config.leadFloorUs, (int64_t)std::llround(config.leadK * (double)worst));
}

int64_t Node::resendIntervalLocked(const Peer & peer) const {
    return std::max(MIN_RESEND_US, peer.stats.delayP99Us + peer.stats.delayP99Us / 4);
}

void Node::resendEventsLocked(int64_t now, int64_t & next) {
    int64_t sharedNow = clock.toShared(now);
    for (auto & kv : pending) {
        Pending & p = kv.second;
        // Until T, to every live follower that hasn't acked.
        if (!p.tracked || p.event.time <= sharedNow) continue;
        for (auto & pk : peers) {
            const Peer & peer = pk.second;
            if (!isLiveLocked(peer, now) || peer.route.empty() || p.acks.count(peer.nodeId)) continue;
            int64_t interval = resendIntervalLocked(peer);
            auto sent = p.lastSent.find(peer.nodeId);
            int64_t last = sent == p.lastSent.end() ? p.broadcastAt : sent->second;
            if (now - last >= interval) {
                queueSendLocked(peer.route, eventMessageLocked(p.event));
                p.lastSent[peer.nodeId] = now;
                last = now;
            }
            next = std::min(next, last + interval);
        }
    }
}

void Node::resendRequestsLocked(int64_t now, int64_t & next) {
    if (role == Role::Master) {
        // Became master with requests still out: carry them out directly.
        for (auto & kv : eventRequests) {
            const Message & m = kv.second.msg;
            Event ev = m.event;
            ev.time = assignTimeLocked(m.event.time, m.leadUs, now);
            masterScheduleLocked(ev, now);
        }
        eventRequests.clear();
        for (auto & kv : timelineRequests) {
            const Message & m = kv.second.msg;
            applyTimelineChangeLocked(m.timeline.id, m.hasPosition, m.timeline.p0, m.hasRate, m.timeline.rate,
                                      m.leadUs, m.timeline.t0, m.requestId, now);
        }
        timelineRequests.clear();
        return;
    }

    const Peer * master = masterId.empty() ? nullptr : findPeerLocked(masterId);
    auto resend = [&](std::map<EventId, OutRequest> & requests, const char * what) {
        for (auto it = requests.begin(); it != requests.end();) {
            OutRequest & r = it->second;
            if (now - r.queuedAt > REQUEST_GIVE_UP_US) {
                ofLogWarning(LOG) << "no master confirmed " << what << " " << it->first.toString() << "; giving up";
                it = requests.erase(it);
                continue;
            }
            if (master && !master->route.empty()) {
                if (!r.sent || now - r.lastSent >= REQUEST_RESEND_US) {
                    r.msg.term = term;
                    queueSendLocked(master->route, r.msg);
                    r.sent = true;
                    r.lastSent = now;
                }
                next = std::min(next, r.lastSent + REQUEST_RESEND_US);
            }
            next = std::min(next, r.queuedAt + REQUEST_GIVE_UP_US + 1);
            ++it;
        }
    };
    resend(eventRequests, "event request");
    resend(timelineRequests, "timeline request");
}

EventId Node::scheduleLocked(const std::string & name, const std::string & payload, int64_t at, int64_t leadUs, LatePolicy policy) {
    int64_t now = localClock->now();

    Event ev;
    ev.id = EventId(nodeId, ++eventSeq);
    ev.name = name;
    ev.payload = payload;
    ev.latePolicy = policy;

    if (role == Role::Master) {
        ev.time = assignTimeLocked(at, leadUs, now);
        masterScheduleLocked(ev, now);
    } else {
        // The master assigns the time, which keeps a single authority on
        // ordering. Until it confirms, the request is resent.
        ev.time = at;
        OutRequest r;
        r.msg = makeLocked(MessageType::EventRequest);
        r.msg.event = ev;
        r.msg.leadUs = leadUs;
        r.queuedAt = now;
        eventRequests[ev.id] = r;
        int64_t ignored = NEVER;
        resendRequestsLocked(now, ignored);
    }
    return ev.id;
}

EventId Node::scheduleEvent(const std::string & name, const std::string & payload, int64_t leadUs, LatePolicy policy) {
    std::vector<Outgoing> out;
    EventId id;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running) {
            ofLogWarning(LOG) << "scheduleEvent() on a stopped node";
            return id;
        }
        id = scheduleLocked(name, payload, 0, std::max<int64_t>(leadUs, 0), policy);
        out.swap(outbox);
    }
    flush(out);
    kickService();
    return id;
}

EventId Node::scheduleEventAt(const std::string & name, int64_t sharedUs, const std::string & payload, LatePolicy policy) {
    std::vector<Outgoing> out;
    EventId id;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running) {
            ofLogWarning(LOG) << "scheduleEventAt() on a stopped node";
            return id;
        }
        id = scheduleLocked(name, payload, std::max<int64_t>(sharedUs, 1), 0, policy);
        out.swap(outbox);
    }
    flush(out);
    kickService();
    return id;
}

std::vector<Event> Node::getPendingEvents() const {
    std::lock_guard<std::mutex> lock(mutex);
    std::vector<Event> result;
    for (auto & kv : pending) {
        if (!kv.second.fired) result.push_back(kv.second.event);
    }
    std::sort(result.begin(), result.end(), [](const Event & a, const Event & b) { return a.time < b.time; });
    return result;
}

// --- timelines ---------------------------------------------------------------

void Node::applyTimelineChangeLocked(const std::string & id, bool hasPosition, int64_t position, bool hasRate, double rate,
                                     int64_t leadUs, int64_t at, const EventId & requestId, int64_t now) {
    int64_t sharedNow = clock.toShared(now);
    int64_t t0 = at > 0 ? std::max(at, sharedNow) : sharedNow + std::max(leadTimeLocked(now), leadUs);

    // Continue from wherever the timeline will be at t0.
    TimelineState base = timelines.stateAt(id, t0);
    TimelineState st;
    st.id = id;
    st.version = timelines.latestVersion(id) + 1;
    st.t0 = t0;
    st.p0 = hasPosition ? position : base.positionAt(t0);
    st.rate = hasRate ? rate : base.rate;
    timelines.apply(st, sharedNow);

    if (!requestId.empty()) handledTimelineRequests[requestId] = now;

    queueBroadcastLocked(timelineMessageLocked(st, requestId));

    // Twice more before it takes effect, in case the first is lost. After
    // that the once-a-beacon refresh covers it.
    int64_t span = clock.toLocal(t0) - now;
    if (span > 3) {
        TimelineResend r;
        r.state = st;
        r.requestId = requestId;
        r.at = {now + span / 3, now + 2 * span / 3};
        timelineResends.push_back(r);
    }
}

void Node::resendTimelinesLocked(int64_t now, int64_t & next) {
    for (auto it = timelineResends.begin(); it != timelineResends.end();) {
        auto & r = *it;
        while (!r.at.empty() && now >= r.at.front()) {
            queueBroadcastLocked(timelineMessageLocked(r.state, r.requestId));
            r.at.erase(r.at.begin());
        }
        if (r.at.empty()) {
            it = timelineResends.erase(it);
        } else {
            next = std::min(next, r.at.front());
            ++it;
        }
    }
}

void Node::changeTimeline(const std::string & id, bool setPosition, int64_t positionUs, bool setRate,
                          double rate, int64_t leadUs, int64_t atSharedUs) {
    if (id.empty()) return;

    std::vector<Outgoing> out;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running) {
            ofLogWarning(LOG) << "timeline change on a stopped node";
            return;
        }
        int64_t now = localClock->now();
        leadUs = std::max<int64_t>(leadUs, 0);

        if (role == Role::Master) {
            applyTimelineChangeLocked(id, setPosition, positionUs, setRate, rate, leadUs, atSharedUs, EventId(), now);
        } else {
            OutRequest r;
            r.msg = makeLocked(MessageType::Timeline);
            r.msg.timeline.id = id;
            r.msg.timeline.version = 0;
            r.msg.timeline.t0 = atSharedUs;
            r.msg.timeline.p0 = positionUs;
            r.msg.timeline.rate = rate;
            r.msg.hasPosition = setPosition;
            r.msg.hasRate = setRate;
            r.msg.leadUs = leadUs;
            r.msg.requestId = EventId(nodeId, ++eventSeq);
            r.queuedAt = now;
            timelineRequests[r.msg.requestId] = r;
            int64_t ignored = NEVER;
            resendRequestsLocked(now, ignored);
        }
        out.swap(outbox);
    }
    flush(out);
    kickService();
}

void Node::play(const std::string & id, int64_t leadUs) {
    changeTimeline(id, false, 0, true, 1.0, leadUs);
}

void Node::pause(const std::string & id, int64_t leadUs) {
    changeTimeline(id, false, 0, true, 0.0, leadUs);
}

void Node::seek(const std::string & id, int64_t positionUs, int64_t leadUs) {
    changeTimeline(id, true, positionUs, false, 0.0, leadUs);
}

void Node::setRate(const std::string & id, double rate, int64_t leadUs) {
    changeTimeline(id, false, 0, true, rate, leadUs);
}

bool Node::hasTimeline(const std::string & id) const {
    std::lock_guard<std::mutex> lock(mutex);
    return timelines.has(id);
}

std::vector<std::string> Node::getTimelineIds() const {
    std::lock_guard<std::mutex> lock(mutex);
    return timelines.ids();
}

TimelineState Node::getTimeline(const std::string & id) const {
    int64_t sharedNow = getSharedTimeUs();
    std::lock_guard<std::mutex> lock(mutex);
    return timelines.stateAt(id, sharedNow);
}

int64_t Node::getTimelinePositionUs(const std::string & id) const {
    return getTimelinePositionUs(id, getOutputTimeUs());
}

int64_t Node::getTimelinePositionUs(const std::string & id, int64_t sharedUs) const {
    std::lock_guard<std::mutex> lock(mutex);
    return timelines.positionAt(id, sharedUs);
}

double Node::getTimelinePosition(const std::string & id) const {
    return (double)getTimelinePositionUs(id) / 1e6;
}

// --- messages ----------------------------------------------------------------

Message Node::makeLocked(MessageType type) const {
    Message m;
    m.type = type;
    m.group = config.group;
    m.nodeId = nodeId;
    m.term = term;
    return m;
}

Message Node::eventMessageLocked(const Event & ev) const {
    Message m = makeLocked(MessageType::Event);
    m.event = ev;
    return m;
}

Message Node::timelineMessageLocked(const TimelineState & st, const EventId & requestId) const {
    Message m = makeLocked(MessageType::Timeline);
    m.timeline = st;
    m.requestId = requestId;
    return m;
}

Message Node::snapshotLocked(int64_t now) const {
    Message m = makeLocked(MessageType::Snapshot);
    m.timelines = timelines.all();
    int64_t sharedNow = clock.toShared(now);
    for (auto & kv : pending) {
        if (kv.second.event.time >= sharedNow) m.events.push_back(kv.second.event);
    }
    return m;
}

void Node::queueBeaconLocked(int64_t now) {
    Message b = makeLocked(MessageType::Beacon);
    b.priority = config.priority;
    b.address = announcedAddress;
    b.port = config.port;
    b.isMaster = role == Role::Master;
    b.masterId = masterId;
    b.leadUs = role == Role::Master ? leadTimeLocked(now) : 0;
    b.stats = ownStatsLocked(now);
    queueBroadcastLocked(b);  // sharedUs is stamped right before it goes

    if (discovery) {
        Outgoing o;
        o.kind = Outgoing::Kind::Announce;
        o.msg = b;
        outbox.push_back(o);
    }
}

void Node::queueSendLocked(const Endpoint & to, const Message & msg) {
    if (to.empty()) return;
    Outgoing o;
    o.kind = Outgoing::Kind::Send;
    o.to = to;
    o.msg = msg;
    outbox.push_back(std::move(o));
}

void Node::queueBroadcastLocked(const Message & msg) {
    Outgoing o;
    o.kind = Outgoing::Kind::Broadcast;
    o.msg = msg;
    outbox.push_back(std::move(o));
}

void Node::flush(std::vector<Outgoing> & out) {
    if (!transport) return;
    for (auto & o : out) {
        // Stamped as late as possible: right before the transport's send.
        switch (o.msg.type) {
            case MessageType::Ping:
                o.msg.t0 = localClock->now();
                break;
            case MessageType::Pong:
                o.msg.t2 = clock.toShared(localClock->now());
                break;
            case MessageType::Beacon:
                o.msg.sharedUs = clock.isLocked() ? clock.toShared(localClock->now()) : 0;
                break;
            default:
                break;
        }
        switch (o.kind) {
            case Outgoing::Kind::Send:
                transport->send(o.to, o.msg);
                break;
            case Outgoing::Kind::Broadcast:
                transport->broadcast(o.msg);
                break;
            case Outgoing::Kind::Announce:
                if (discovery) discovery->announce(o.msg);
                break;
        }
    }
    out.clear();
}

// --- notifications -----------------------------------------------------------

void Node::noteRoleLocked() {
    Notification n;
    n.roleChange.role = role;
    n.roleChange.masterId = masterId;
    n.roleChange.term = term;
    notifications.push_back(n);
    while (notifications.size() > MAX_NOTIFICATIONS) notifications.pop_front();
}

void Node::noteDiscontinuityLocked(Discontinuity::Reason reason, int64_t stepUs) {
    Notification n;
    n.isDiscontinuity = true;
    n.discontinuity.reason = reason;
    n.discontinuity.stepUs = stepUs;
    n.discontinuity.masterId = masterId;
    n.discontinuity.term = term;
    notifications.push_back(n);
    while (notifications.size() > MAX_NOTIFICATIONS) notifications.pop_front();
}

void Node::kickScheduler() {
    {
        std::lock_guard<std::mutex> lock(schedulerWaitMutex);
        schedulerKicked = true;
    }
    schedulerCv.notify_one();
}

void Node::kickService() {
    {
        std::lock_guard<std::mutex> lock(serviceWaitMutex);
        serviceKicked = true;
    }
    serviceCv.notify_one();
}

void Node::update() {
    std::deque<FiredEvent> fired;
    std::deque<Notification> notes;
    {
        std::lock_guard<std::mutex> lock(mutex);
        fired.swap(mainQueue);
        notes.swap(notifications);
    }

    for (auto & n : notes) {
        if (n.isDiscontinuity) {
            const Discontinuity & d = n.discontinuity;
            ofNotifyEvent(onDiscontinuity, d, this);
        } else {
            const RoleChange & r = n.roleChange;
            ofNotifyEvent(onRoleChange, r, this);
        }
    }

    if (fired.empty()) return;
    int64_t sharedNow = getSharedTimeUs();
    for (auto & f : fired) {
        f.latenessUs = sharedNow - (f.event.time - config.outputLatencyUs);
        const FiredEvent & e = f;
        ofNotifyEvent(onEvent, e, this);
    }
}

// --- periodic work -----------------------------------------------------------

int64_t Node::poll() {
    return poll(localClock->now());
}

int64_t Node::poll(int64_t now) {
    int64_t a = service(now);
    int64_t b = dispatch(now);
    return std::min(a, b);
}

int64_t Node::service(int64_t now) {
    std::vector<Endpoint> found = discovery ? discovery->getEndpoints() : std::vector<Endpoint>();
    std::vector<Outgoing> out;
    std::vector<std::string> lines;
    std::vector<Endpoint> candidates;
    std::set<Endpoint> self;
    int64_t next = now + MAX_SERVICE_WAIT_US;

    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running) return NEVER;

        // A starting node listens for an existing master before it joins an
        // election, so a returning node joins as a follower.
        if (role == Role::Starting) {
            int64_t end = startedAt + 2 * config.beaconIntervalUs;
            if (now >= end) {
                role = Role::Follower;
                electionSince = now;
                noteRoleLocked();
            } else {
                next = std::min(next, end);
            }
        }

        if (role == Role::Follower && !masterId.empty()) {
            const Peer * m = findPeerLocked(masterId);
            if (!m || !isLiveLocked(*m, now)) masterLostLocked(now);
        }
        if (role == Role::Follower && masterId.empty()) {
            electLocked(now);
        }

        // Wake when a peer would time out, so a lost master is noticed on time.
        for (auto & kv : peers) {
            if (isLiveLocked(kv.second, now)) next = std::min(next, kv.second.lastHeard + config.masterTimeoutUs);
        }

        if (now >= nextBeaconAt) {
            if (!config.interface.empty()) announcedAddress = net::interfaceAddress(config.interface);
            queueBeaconLocked(now);
            // Timelines are state, so the master repeats them: a lost change
            // heals within a beacon.
            if (role == Role::Master) {
                for (auto & st : timelines.all()) queueBroadcastLocked(timelineMessageLocked(st, EventId()));
            }
            nextBeaconAt = now + config.beaconIntervalUs;
        }
        next = std::min(next, nextBeaconAt);

        if (role == Role::Follower && !masterId.empty()) syncServiceLocked(now, next);
        if (role == Role::Master) {
            resendEventsLocked(now, next);
            resendTimelinesLocked(now, next);
        }
        resendRequestsLocked(now, next);
        pruneLocked(now);

        candidates = found;
        for (auto & kv : peers) {
            if (isLiveLocked(kv.second, now) && !kv.second.route.empty()) candidates.push_back(kv.second.route);
        }
        self = selfEndpoints;
        out.swap(outbox);
        lines.swap(csvLines);
    }

    // Broadcasts reach discovery's endpoints plus every live peer, never
    // this node itself.
    std::vector<Endpoint> targets;
    for (auto & e : candidates) {
        if (self.count(e) || transport->isLocal(e)) continue;
        if (std::find(targets.begin(), targets.end(), e) == targets.end()) targets.push_back(e);
    }
    std::sort(targets.begin(), targets.end());
    if (targets != transportPeers) {
        transportPeers = targets;
        transport->setPeers(targets);
    }

    flush(out);

    if (!lines.empty() && csv.is_open()) {
        for (auto & l : lines) csv << l << '\n';
        csv.flush();
    }
    return next;
}

void Node::pruneLocked(int64_t now) {
    for (auto it = done.begin(); it != done.end();) {
        it = now - it->second > DONE_RETENTION_US ? done.erase(it) : std::next(it);
    }
    for (auto it = outstandingPings.begin(); it != outstandingPings.end();) {
        it = now - it->second > PING_RETENTION_US ? outstandingPings.erase(it) : std::next(it);
    }
    for (auto it = handledTimelineRequests.begin(); it != handledTimelineRequests.end();) {
        it = now - it->second > REQUEST_RETENTION_US ? handledTimelineRequests.erase(it) : std::next(it);
    }

    if (clock.isLocked()) {
        int64_t sharedNow = clock.toShared(now);
        for (auto it = pending.begin(); it != pending.end();) {
            const Pending & p = it->second;
            // The master keeps an event until T for resends, even if its own
            // output latency fired it early.
            if (p.fired && (!p.tracked || p.event.time <= sharedNow)) {
                done[it->first] = now;
                it = pending.erase(it);
            } else {
                ++it;
            }
        }
        timelines.prune(sharedNow);
    }

    for (auto it = peers.begin(); it != peers.end();) {
        const Peer & p = it->second;
        bool forget = p.nodeId != masterId && (!p.heard || now - p.lastHeard > PEER_RETENTION_US);
        it = forget ? peers.erase(it) : std::next(it);
    }
}

void Node::logCsvLocked(int64_t now, const std::string & kind, const std::string & who, const std::string & master,
                        int64_t peerTerm, int64_t offsetUs, const SyncStats & stats) {
    if (!csv.is_open()) return;
    std::ostringstream line;
    line << now << ',' << (clock.isLocked() ? clock.toShared(now) : 0) << ',' << kind << ',' << who << ','
         << master << ',' << peerTerm << ',' << offsetUs << ',' << stats.delayUs << ',' << stats.errorUs << ','
         << stats.driftPpm << ',' << (stats.locked ? 1 : 0);
    csvLines.push_back(line.str());
}

// --- dispatch ----------------------------------------------------------------

int64_t Node::dispatch(int64_t now) {
    std::vector<FiredEvent> fire;
    int64_t next = NEVER;
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (!running || !clock.isLocked()) return NEVER;

        int64_t sharedNow = clock.toShared(now);
        for (auto & kv : pending) {
            Pending & p = kv.second;
            if (p.fired) continue;

            // Early by the output latency, so what it drives lands on time.
            int64_t target = p.event.time - config.outputLatencyUs;
            if (sharedNow < target) {
                next = std::min(next, clock.toLocal(target));
                continue;
            }

            p.fired = true;
            int64_t lateness = sharedNow - target;
            if (lateness > MAX_EVENT_AGE_US) {
                // Left behind by a clock step: from another timebase.
                eventsDropped++;
                ofLogWarning(LOG) << "dropped event " << p.event.name << ": over a minute late";
                continue;
            }
            if (p.event.latePolicy == LatePolicy::DropLate && lateness > config.lateToleranceUs) {
                eventsDropped++;
                ofLogWarning(LOG) << "dropped event " << p.event.name << ": " << ms(lateness) << " late";
                continue;
            }
            eventsFired++;
            FiredEvent f;
            f.event = p.event;
            f.latenessUs = lateness;
            fire.push_back(std::move(f));
        }
    }

    if (fire.empty()) return next;

    std::sort(fire.begin(), fire.end(), [](const FiredEvent & a, const FiredEvent & b) {
        return std::tie(a.event.time, a.event.id) < std::tie(b.event.time, b.event.id);
    });
    for (auto & f : fire) {
        const FiredEvent & e = f;
        ofNotifyEvent(onEventThread, e, this);
    }

    std::lock_guard<std::mutex> lock(mutex);
    for (auto & f : fire) mainQueue.push_back(std::move(f));
    while (mainQueue.size() > MAX_MAIN_QUEUE) mainQueue.pop_front();
    return next;
}

void Node::serviceLoop() {
    while (threadsRunning) {
        int64_t next = service(localClock->now());
        int64_t now = localClock->now();
        int64_t wake = std::min(next, now + MAX_SERVICE_WAIT_US);
        int64_t waitUs = localClock->toMonotonic(wake) - monotonicUs();

        std::unique_lock<std::mutex> lock(serviceWaitMutex);
        if (!serviceKicked && waitUs > 0) {
            serviceCv.wait_for(lock, std::chrono::microseconds(waitUs), [this] {
                return serviceKicked || !threadsRunning;
            });
        }
        serviceKicked = false;
    }
}

void Node::schedulerLoop() {
#ifdef __linux__
    if (config.realtimePriority > 0) {
        sched_param sp{};
        sp.sched_priority = std::clamp(config.realtimePriority, 1, 99);
        int err = pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp);
        if (err) {
            ofLogWarning(LOG) << "couldn't run the scheduler SCHED_FIFO: " << strerror(err)
                              << " (needs an rtprio limit or CAP_SYS_NICE)";
        }
    }
#endif

    while (threadsRunning) {
        int64_t deadline = dispatch(localClock->now());
        int64_t now = localClock->now();

        if (deadline != NEVER && deadline - now <= FINE_SLEEP_US) {
            // The last stretch: sleep straight to the deadline.
            if (deadline > now) sleepUntilMonotonicUs(localClock->toMonotonic(deadline));
            continue;
        }

        // Otherwise wait somewhere that a new event or a clock step can
        // interrupt, and recompute: the deadline moves as the clock slews.
        int64_t wake = deadline == NEVER ? now + MAX_SCHEDULER_WAIT_US
                                         : std::min(deadline - FINE_SLEEP_US, now + MAX_SCHEDULER_WAIT_US);
        int64_t waitUs = localClock->toMonotonic(wake) - monotonicUs();

        std::unique_lock<std::mutex> lock(schedulerWaitMutex);
        if (!schedulerKicked && waitUs > 0) {
            schedulerCv.wait_for(lock, std::chrono::microseconds(waitUs), [this] {
                return schedulerKicked || !threadsRunning;
            });
        }
        schedulerKicked = false;
    }
}

// --- getters -----------------------------------------------------------------

int64_t Node::getLocalTimeUs() const {
    return localClock->now();
}

int64_t Node::getSharedTimeUs() const {
    return clock.toShared(localClock->now());
}

double Node::getSharedTime() const {
    return (double)getSharedTimeUs() / 1e6;
}

int64_t Node::getOutputTimeUs() const {
    return getSharedTimeUs() + config.outputLatencyUs;
}

int64_t Node::sharedFromLocal(int64_t localUs) const {
    return clock.toShared(localUs);
}

int64_t Node::localFromShared(int64_t sharedUs) const {
    return clock.toLocal(sharedUs);
}

bool Node::isLocked() const {
    return clock.isLocked();
}

Role Node::getRole() const {
    std::lock_guard<std::mutex> lock(mutex);
    return role;
}

bool Node::isMaster() const {
    std::lock_guard<std::mutex> lock(mutex);
    return role == Role::Master;
}

std::string Node::getMasterId() const {
    std::lock_guard<std::mutex> lock(mutex);
    return masterId;
}

int64_t Node::getTerm() const {
    std::lock_guard<std::mutex> lock(mutex);
    return term;
}

int64_t Node::getLeadTimeUs() const {
    int64_t now = localClock->now();
    std::lock_guard<std::mutex> lock(mutex);
    return leadTimeLocked(now);
}

Status Node::getStatus() const {
    int64_t now = localClock->now();
    std::lock_guard<std::mutex> lock(mutex);
    Status s;
    s.role = role;
    s.nodeId = nodeId;
    s.masterId = masterId;
    s.term = term;
    s.sharedUs = clock.toShared(now);
    s.leadUs = leadTimeLocked(now);
    s.slewRemainingUs = clock.remainingSlewUs(now);
    for (auto & kv : pending) {
        if (!kv.second.fired) s.pendingEvents++;
    }
    s.eventsFired = eventsFired;
    s.eventsDropped = eventsDropped;
    s.stats = ownStatsLocked(now);
    return s;
}

std::vector<PeerStatus> Node::getPeers() const {
    int64_t now = localClock->now();
    std::lock_guard<std::mutex> lock(mutex);
    std::vector<PeerStatus> result;
    for (auto & kv : peers) {
        const Peer & p = kv.second;
        PeerStatus s;
        s.nodeId = p.nodeId;
        s.route = p.route;
        s.priority = p.priority;
        s.isMaster = p.isMaster;
        s.masterId = p.masterId;
        s.term = p.term;
        s.live = isLiveLocked(p, now);
        s.lastHeardAgoUs = p.heard ? now - p.lastHeard : -1;
        s.stats = p.stats;
        result.push_back(s);
    }
    return result;
}

} // namespace ofxSyncing
