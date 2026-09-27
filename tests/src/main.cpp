#include "ofAppNoWindow.h"
#include "ofMain.h"

#include "ofxUnitTests.h"

#include "ofxSyncing.h"
#include "ofxSyncingOsc.h"

using namespace ofxSyncing;

namespace {

constexpr int64_t MS = 1000;
constexpr int64_t SEC = 1000000;

Config nodeConfig(const std::string & id, int priority = 100) {
    Config c;
    c.group = "test";
    c.nodeId = id;
    c.priority = priority;
    return c;
}

// A PING/PONG exchange with a known offset and one-way delays.
ClockSample exchange(int64_t local, int64_t offset, int64_t out, int64_t back, int64_t hold = 20) {
    ClockSample s;
    s.t0 = local;
    s.t1 = local + out + offset;
    s.t2 = s.t1 + hold;
    s.t3 = local + out + hold + back;
    return s;
}

int64_t percentile(std::vector<int64_t> v, double p) {
    if (v.empty()) return 0;
    std::sort(v.begin(), v.end());
    size_t rank = (size_t)std::ceil(p * (double)v.size());
    return v[std::clamp<size_t>(rank, 1, v.size()) - 1];
}

std::string ms(int64_t us) {
    return ofToString((double)us / 1000.0, 3) + " ms";
}

// Every event firing across a simulation: which node, which event, and the
// true time it fired at.
class Recorder {
public:
    struct Fire {
        std::string node;
        EventId id;
        std::string name;
        int64_t trueUs;
        int64_t latenessUs;
    };

    void watch(Simulation & sim, Node & node) {
        std::string id = node.getNodeId();
        listeners.push(node.onEventThread.newListener([this, &sim, id](const FiredEvent & e) {
            fires.push_back({id, e.event.id, e.event.name, sim.now(), e.latenessUs});
        }));
    }

    int count(const EventId & id, const std::string & node) const {
        int n = 0;
        for (auto & f : fires) {
            if (f.id == id && f.node == node) n++;
        }
        return n;
    }

    // For each event, the spread of the true times the nodes fired it at.
    int64_t worstSpread() const {
        std::map<EventId, std::pair<int64_t, int64_t>> range;
        for (auto & f : fires) {
            auto it = range.find(f.id);
            if (it == range.end()) {
                range[f.id] = {f.trueUs, f.trueUs};
            } else {
                it->second.first = std::min(it->second.first, f.trueUs);
                it->second.second = std::max(it->second.second, f.trueUs);
            }
        }
        int64_t worst = 0;
        for (auto & r : range) worst = std::max(worst, r.second.second - r.second.first);
        return worst;
    }

    std::vector<Fire> fires;

private:
    ofEventListeners listeners;
};

bool sameMessage(const Message & a, const Message & b) {
    auto sameEvent = [](const Event & x, const Event & y) {
        return x.id == y.id && x.name == y.name && x.payload == y.payload && x.time == y.time && x.latePolicy == y.latePolicy;
    };
    auto sameTimeline = [](const TimelineState & x, const TimelineState & y) {
        return x.id == y.id && x.version == y.version && x.t0 == y.t0 && x.p0 == y.p0 && x.rate == y.rate;
    };

    if (a.type != b.type || a.group != b.group || a.nodeId != b.nodeId || a.term != b.term) return false;
    switch (a.type) {
        case MessageType::Beacon:
            return a.priority == b.priority && a.address == b.address && a.port == b.port && a.isMaster == b.isMaster &&
                   a.masterId == b.masterId && a.sharedUs == b.sharedUs && a.leadUs == b.leadUs &&
                   a.stats.locked == b.stats.locked && a.stats.errorUs == b.stats.errorUs &&
                   a.stats.delayUs == b.stats.delayUs && a.stats.delayP99Us == b.stats.delayP99Us &&
                   a.stats.driftPpm == b.stats.driftPpm;
        case MessageType::Ping:
            return a.seq == b.seq && a.t0 == b.t0 && a.wantSnapshot == b.wantSnapshot;
        case MessageType::Pong:
            return a.seq == b.seq && a.t0 == b.t0 && a.t1 == b.t1 && a.t2 == b.t2;
        case MessageType::Event:
            return sameEvent(a.event, b.event);
        case MessageType::Ack:
            return a.event.id == b.event.id;
        case MessageType::EventRequest:
            return sameEvent(a.event, b.event) && a.leadUs == b.leadUs;
        case MessageType::Timeline:
            return sameTimeline(a.timeline, b.timeline) && a.hasPosition == b.hasPosition && a.hasRate == b.hasRate &&
                   a.requestId == b.requestId && a.leadUs == b.leadUs;
        case MessageType::Snapshot:
            if (a.timelines.size() != b.timelines.size() || a.events.size() != b.events.size()) return false;
            for (size_t i = 0; i < a.timelines.size(); i++) {
                if (!sameTimeline(a.timelines[i], b.timelines[i])) return false;
            }
            for (size_t i = 0; i < a.events.size(); i++) {
                if (!sameEvent(a.events[i], b.events[i])) return false;
            }
            return true;
    }
    return false;
}

std::vector<Message> sampleMessages() {
    std::vector<Message> all;
    auto make = [](MessageType t) {
        Message m;
        m.type = t;
        m.group = "gallery";
        m.nodeId = "pi-3";
        m.term = 7;
        return m;
    };

    Event ev;
    ev.id = EventId("pi-1", 281474976710000ull);
    ev.name = "flash";
    ev.payload = "{\"color\":\"red\"}";
    ev.time = 1234567890123;
    ev.latePolicy = LatePolicy::DropLate;

    TimelineState tl;
    tl.id = "show";
    tl.version = 12;
    tl.t0 = 987654321012;
    tl.p0 = -2500000;
    tl.rate = 1.5;

    Message beacon = make(MessageType::Beacon);
    beacon.priority = 250;
    beacon.address = "10.0.0.12";
    beacon.port = 9400;
    beacon.isMaster = true;
    beacon.masterId = "pi-3";
    beacon.sharedUs = 5555555555555;
    beacon.leadUs = 50000;
    beacon.stats.locked = true;
    beacon.stats.errorUs = 123;
    beacon.stats.delayUs = 456;
    beacon.stats.delayP99Us = 789;
    beacon.stats.driftPpm = -42.25;
    all.push_back(beacon);

    Message ping = make(MessageType::Ping);
    ping.seq = 4000000000u;
    ping.t0 = 111111111111;
    ping.wantSnapshot = true;
    all.push_back(ping);

    Message pong = make(MessageType::Pong);
    pong.seq = 99;
    pong.t0 = 111111111111;
    pong.t1 = 222222222222;
    pong.t2 = 222222222333;
    all.push_back(pong);

    Message event = make(MessageType::Event);
    event.event = ev;
    all.push_back(event);

    Message ack = make(MessageType::Ack);
    ack.event.id = ev.id;
    all.push_back(ack);

    Message request = make(MessageType::EventRequest);
    request.event = ev;
    request.event.payload = std::string("bin\0ary", 7);  // OSC carries it as a blob
    request.leadUs = 250000;
    all.push_back(request);

    Message timeline = make(MessageType::Timeline);
    timeline.timeline = tl;
    timeline.requestId = EventId("pi-2", 17);
    all.push_back(timeline);

    Message tlRequest = make(MessageType::Timeline);
    tlRequest.timeline = tl;
    tlRequest.timeline.version = 0;
    tlRequest.hasPosition = true;
    tlRequest.hasRate = true;
    tlRequest.requestId = EventId("pi-2", 18);
    tlRequest.leadUs = 100000;
    all.push_back(tlRequest);

    Message snapshot = make(MessageType::Snapshot);
    snapshot.timelines = {tl, tl};
    snapshot.timelines[1].id = "lights";
    snapshot.events = {ev, ev};
    snapshot.events[1].id.seq++;
    all.push_back(snapshot);

    Message empty = make(MessageType::Snapshot);
    all.push_back(empty);

    return all;
}

} // namespace

class ofApp: public ofxUnitTestsApp {
    void run() override {
        // The protocol's own notices would bury the results.
        ofSetLogLevel("ofxSyncing", OF_LOG_ERROR);

        testOffsetMath();
        testSampleFiltering();
        testDriftFit();
        testEarlyDrift();
        testSlewAndStep();
        testTimelinePosition();
        testTimelineVersions();
        testJsonCodec();
        testOscCodec();
        testMasterSelection();
        testLockOnLan();
        testLockWithNetem();
        testLockOnWifi();
        testEventsWithLoss();
        testFollowerRequests();
        testDeduplication();
        testLatePolicies();
        testOutputLatency();
        testTimelinesAcrossNodes();
        testLateJoin();
        testFailover();
        testReturningNodeWithLaterTerm();
        testReorderedBeacon();
        testSplitHeal();
        testLongRunDrift();
        testRealThreadsOverOsc();
    }

    // --- clock math ---

    void testOffsetMath() {
        // The master is 5 ms ahead; 100 µs each way; it holds the ping 20 µs.
        ClockSample s = exchange(1000000, 5000, 100, 100);
        ofxTestEq(s.offset(), 5000, "offset from a symmetric exchange");
        ofxTestEq(s.delay(), 200, "delay leaves out the master's hold time");

        // 300 µs out, 100 back: half the asymmetry shows up as error.
        ClockSample a = exchange(1000000, 5000, 300, 100);
        ofxTestEq(a.offset(), 5100, "an asymmetric path biases the offset by half the difference");
        ofxTestEq(a.delay(), 400, "delay of an asymmetric path");

        ofxTestEq(Endpoint::parse("pi2.local", 9400).toString(), "pi2.local:9400", "endpoint without a port");
        ofxTestEq(Endpoint::parse(" 10.0.0.2:9500 ", 9400).toString(), "10.0.0.2:9500", "endpoint with a port");
        ofxTestEq(Endpoint::parse("[fe80::1]:9500", 9400).host, "fe80::1", "IPv6 endpoint");
    }

    void testSampleFiltering() {
        std::mt19937 rng(7);
        ClockEstimator est;
        int64_t t = 10 * SEC;
        for (int i = 0; i < 32; i++, t += SEC) {
            // A third of the samples spike in one direction, as on WiFi.
            bool spike = i % 3 == 0;
            int64_t out = 100 + (spike ? 15000 : (int64_t)(rng() % 40));
            int64_t back = 100 + (int64_t)(rng() % 40);
            est.addSample(exchange(t, 5000, out, back));
        }
        auto e = est.estimate(t);
        ofxTest(e.valid, "estimate from a window");
        ofxTestLt(std::llabs(e.offsetUs - 5000), 40, "the min-delay filter discards one-way spikes");
        ofxTestGt(e.p99DelayUs, 10000, "p99 delay sees the spikes");

        std::vector<ClockSample> burst;
        for (int i = 0; i < 8; i++) {
            int64_t out = i < 6 ? 100 + 8000 * (i + 1) : 100;
            burst.push_back(exchange(t + i * 50000, 5000, out, 100));
        }
        auto b = est.burstEstimate(burst, t);
        ofxTestLt(std::llabs(b.offsetUs - 5000), 5, "a burst locks on its lowest-delay quarter");
    }

    void testDriftFit() {
        std::mt19937 rng(11);
        ClockEstimator est;
        const double drift = 80e-6;
        int64_t t = 0;
        for (int i = 0; i < 180; i++, t += SEC) {
            int64_t offset = 5000 + (int64_t)std::llround(drift * (double)t);
            int64_t out = 100 + (int64_t)(rng() % 60) + (i % 5 == 0 ? 3000 : 0);
            int64_t back = 100 + (int64_t)(rng() % 60);
            est.addSample(exchange(t, offset, out, back));
        }
        ofxTest(est.hasSkew(), "drift estimated after three minutes");
        ofxTestLt(std::fabs(est.getSkew() - drift) * 1e6, 2.0, "drift within 2 ppm");

        int64_t truth = 5000 + (int64_t)std::llround(drift * (double)t);
        ofxTestLt(std::llabs(est.estimate(t).offsetUs - truth), 50, "old samples are carried forward with the drift");
    }

    void testEarlyDrift() {
        // A burst, then a ping a second, on a quiet wired link.
        std::mt19937 rng(5);
        ClockEstimator est;
        const double drift = 80e-6;
        int64_t t = 50 * SEC;
        auto add = [&](int64_t at) {
            int64_t offset = 5000 + (int64_t)std::llround(drift * (double)at);
            est.addSample(exchange(at, offset, 70 + (int64_t)(rng() % 40), 70 + (int64_t)(rng() % 40)));
        };
        for (int i = 0; i < 8; i++) add(t + i * 50 * MS);
        int drifted = -1;
        for (int i = 1; i <= 15; i++) {
            add(t + i * SEC);
            if (drifted < 0 && est.hasSkew()) drifted = i;
        }
        ofLogNotice("tests") << "early drift: estimated after " << drifted << " s as " << est.getSkew() * 1e6 << " ppm";
        ofxTest(drifted > 0 && drifted <= 10, "on a quiet link, drift is estimated within 10 s");
        ofxTestLt(std::fabs(est.getSkew() - drift) * 1e6, 10.0, "provisional drift within 10 ppm");
    }

    void testSlewAndStep() {
        SharedClock c;
        c.setSlewRate(500);
        c.setStepThreshold(20 * MS);

        ofxTest(!c.isLocked(), "a new clock is unlocked");
        auto first = c.steer(0, 1000, 0.0);
        ofxTest(first.stepped && c.isLocked(), "the first lock jumps");
        ofxTestEq(c.toShared(0), 1000, "locked offset");

        // 5 ms of error slews in at 500 µs/s: 10 seconds.
        auto slew = c.steer(1 * SEC, 6000, 0.0);
        ofxTest(!slew.stepped, "a small error slews");
        ofxTestEq(slew.errorUs, 5000, "slew error");
        ofxTestEq(c.toShared(1 * SEC), 1 * SEC + 1000, "shared time is continuous at a correction");
        ofxTestEq(c.toShared(6 * SEC), 6 * SEC + 3500, "halfway through the slew");
        ofxTestEq(c.remainingSlewUs(6 * SEC), 2500, "remaining slew");
        ofxTestEq(c.toShared(11 * SEC), 11 * SEC + 6000, "slew complete");
        ofxTestEq(c.toShared(20 * SEC), 20 * SEC + 6000, "no overshoot");

        bool monotonic = true, inverse = true;
        for (int64_t L = 0; L < 20 * SEC; L += 997) {
            if (c.toShared(L + 997) <= c.toShared(L)) monotonic = false;
            int64_t S = c.toShared(L) + 123;
            int64_t back = c.toLocal(S);
            if (c.toShared(back) < S || c.toShared(back - 1) >= S) inverse = false;
        }
        ofxTest(monotonic, "slewing never runs shared time backwards");
        ofxTest(inverse, "toLocal inverts toShared through a slew");

        // Slewing backwards still runs forwards, just slower.
        c.steer(20 * SEC, 1000, 0.0);
        bool forward = true;
        for (int64_t L = 20 * SEC; L < 32 * SEC; L += 1000) {
            if (c.toShared(L + 1000) <= c.toShared(L)) forward = false;
        }
        ofxTest(forward, "a negative correction slows shared time without reversing it");
        ofxTestEq(c.toShared(40 * SEC), 40 * SEC + 1000, "negative slew complete");

        auto step = c.steer(40 * SEC, 1000 + 25 * MS, 0.0);
        ofxTest(step.stepped, "an error past the threshold steps");
        ofxTestEq(step.stepUs, 25 * MS, "step size");

        // Drift carries the offset forward.
        c.steer(50 * SEC, 0, 100e-6, true);
        ofxTestEq(c.toShared(60 * SEC) - 60 * SEC, 1000, "100 ppm is 1 ms per 10 s");
    }

    // --- timelines ---

    void testTimelinePosition() {
        TimelineState st;
        st.id = "t";
        st.version = 1;
        st.t0 = 1 * SEC;
        st.p0 = 0;
        st.rate = 1.0;
        ofxTestEq(st.positionAt(1500 * MS), 500 * MS, "playing position");
        st.rate = 2.0;
        ofxTestEq(st.positionAt(1500 * MS), 1000 * MS, "double speed");
        st.rate = 0.0;
        st.p0 = 42;
        ofxTestEq(st.positionAt(99 * SEC), 42, "paused position doesn't move");
        st.rate = -1.0;
        st.p0 = 10 * SEC;
        ofxTestEq(st.positionAt(3 * SEC), 8 * SEC, "reverse");
    }

    void testTimelineVersions() {
        TimelineSet set;
        TimelineState play{"show", 1, 1 * SEC, 0, 1.0};
        TimelineState pause{"show", 2, 3 * SEC, 2 * SEC, 0.0};
        ofxTest(set.apply(play, 0), "store a state");
        ofxTest(set.apply(pause, 0), "store a later state");
        ofxTest(!set.apply(play, 0), "a known version is ignored");

        ofxTestEq(set.positionAt("show", 500 * MS), 0, "before the first state takes effect");
        ofxTestEq(set.positionAt("show", 2 * SEC), 1 * SEC, "playing");
        ofxTestEq(set.positionAt("show", 4 * SEC), 2 * SEC, "paused");

        set.prune(4 * SEC);
        ofxTestEq(set.all().size(), (size_t)1, "superseded states are pruned");
        ofxTest(!set.apply(play, 4 * SEC), "a superseded state isn't taken back");

        // A newer version cancels an older one that hasn't taken effect.
        TimelineState later{"show", 3, 10 * SEC, 0, 1.0};
        TimelineState sooner{"show", 4, 5 * SEC, 7 * SEC, 1.0};
        set.apply(later, 4 * SEC);
        set.apply(sooner, 4 * SEC);
        ofxTestEq(set.positionAt("show", 6 * SEC), 8 * SEC, "the newer change takes effect");
        ofxTestEq(set.positionAt("show", 11 * SEC), 13 * SEC, "and the older one never does");
    }

    // --- wire formats ---

    void testJsonCodec() {
        bool all = true;
        for (auto & m : sampleMessages()) {
            if (m.type == MessageType::EventRequest) continue;  // binary payload isn't JSON's job
            Message back;
            if (!decodeJson(encodeJson(m), back) || !sameMessage(m, back)) {
                ofLogError("tests") << "JSON round trip failed for " << toString(m.type) << ": " << encodeJson(m);
                all = false;
            }
        }
        ofxTest(all, "every message type survives JSON");

        Message fromBrowser;
        bool parsed = decodeJson("{\"type\":\"ping\",\"group\":\"g\",\"node\":\"web\",\"term\":2,\"seq\":5,\"t0\":1234.7}", fromBrowser);
        ofxTest(parsed && fromBrowser.t0 == 1235, "a browser's float time is accepted");

        Message junk;
        ofxTest(!decodeJson("{\"type\":\"nope\",\"node\":\"x\"}", junk), "unknown types are rejected");
        ofxTest(!decodeJson("not json", junk), "garbage is rejected");
    }

    void testOscCodec() {
        bool all = true;
        for (auto & m : sampleMessages()) {
            ofxOscMessage osc = OscTransport::encode(m, 9400);
            Message back;
            int reply = 0;
            if (!OscTransport::decode(osc, back, reply) || reply != 9400 || !sameMessage(m, back)) {
                ofLogError("tests") << "OSC round trip failed for " << toString(m.type);
                all = false;
            }
        }
        ofxTest(all, "every message type survives OSC");

        ofxOscMessage other;
        other.setAddress("/somebody/else");
        Message m;
        int reply;
        ofxTest(!OscTransport::decode(other, m, reply), "other OSC traffic is ignored");
    }

    // --- simulated groups ---

    void testMasterSelection() {
        Simulation sim(1);
        sim.addNode(nodeConfig("a", 100));
        sim.addNode(nodeConfig("b", 200));
        sim.addNode(nodeConfig("c", 200));
        sim.addNode(nodeConfig("viewer", 0));
        sim.runFor(5 * SEC);

        bool agree = true, locked = true;
        for (auto n : sim.nodes()) {
            if (n->getMasterId() != "c" || n->getTerm() != 1) agree = false;
            if (!n->isLocked()) locked = false;
        }
        ofxTest(sim.node("c")->isMaster(), "the highest priority wins, ties to the higher node id");
        ofxTest(agree, "everyone agrees on the master and term");
        ofxTest(locked, "everyone locks");
    }

    // Warms up, then samples the spread between shared clocks.
    std::vector<int64_t> measureSpread(Simulation & sim, int64_t warmupUs, int64_t runUs, int64_t everyUs) {
        sim.runFor(warmupUs);
        std::vector<int64_t> spreads;
        sim.runFor(runUs, [&] { spreads.push_back(sim.spreadUs()); }, everyUs);
        return spreads;
    }

    void testLockOnLan() {
        Simulation sim(2);
        Simulation::Link wired;
        wired.delayUs = 150;
        wired.jitterUs = 50;
        sim.setDefaultLink(wired);
        sim.addNode(nodeConfig("a", 400), 7 * SEC, -60.0);
        sim.addNode(nodeConfig("b", 300), 1234 * SEC, 35.0);
        sim.addNode(nodeConfig("c", 200), 5, 110.0);
        sim.addNode(nodeConfig("d", 100), 99999 * SEC, -95.0);

        auto spreads = measureSpread(sim, 60 * SEC, 120 * SEC, 100 * MS);
        int64_t p95 = percentile(spreads, 0.95);
        ofLogNotice("tests") << "wired LAN: p95 spread " << ms(p95) << ", max " << ms(percentile(spreads, 1.0));
        ofxTestLt(p95, 1 * MS, "wired: p95 spread under 1 ms");
    }

    void testLockWithNetem() {
        // tc qdisc add dev eth0 root netem delay 20ms 10ms loss 5%
        Simulation sim(3);
        Simulation::Link netem;
        netem.delayUs = 20 * MS;
        netem.jitterUs = 10 * MS;
        netem.loss = 0.05;
        sim.setDefaultLink(netem);
        sim.addNode(nodeConfig("a", 300), 0, 20.0);
        sim.addNode(nodeConfig("b", 200), 55 * SEC, -80.0);
        sim.addNode(nodeConfig("c", 100), 3 * SEC, 70.0);

        auto spreads = measureSpread(sim, 120 * SEC, 300 * SEC, 250 * MS);
        int64_t p95 = percentile(spreads, 0.95);
        ofLogNotice("tests") << "netem 20±10 ms, 5% loss: p95 spread " << ms(p95);
        ofxTestLt(p95, 5 * MS, "netem: p95 spread under 5 ms");
        ofxTestGt(sim.node("a")->getLeadTimeUs(), 50 * MS, "lead time grows with the delay");
    }

    void testLockOnWifi() {
        // Power saving off: a few ms, with spikes that often go only one way.
        Simulation sim(4);
        Simulation::Link wifi;
        wifi.delayUs = 2 * MS;
        wifi.jitterUs = 1 * MS;
        wifi.loss = 0.01;
        wifi.spikeChance = 0.2;
        wifi.spikeUs = 40 * MS;
        sim.setDefaultLink(wifi);
        sim.addNode(nodeConfig("a", 300), 0, 25.0);
        sim.addNode(nodeConfig("b", 200), 21 * SEC, -40.0);
        sim.addNode(nodeConfig("c", 100), 8 * SEC, 90.0);

        auto spreads = measureSpread(sim, 90 * SEC, 300 * SEC, 250 * MS);
        int64_t p95 = percentile(spreads, 0.95);
        ofLogNotice("tests") << "WiFi-like: p95 spread " << ms(p95);
        ofxTestLt(p95, 5 * MS, "WiFi: p95 spread under 5 ms");
    }

    void testEventsWithLoss() {
        Simulation sim(5);
        Simulation::Link lossy;
        lossy.delayUs = 1 * MS;
        lossy.jitterUs = 500;
        lossy.loss = 0.05;
        sim.setDefaultLink(lossy);
        Recorder rec;
        for (auto & id : {"a", "b", "c", "d"}) {
            rec.watch(sim, sim.addNode(nodeConfig(id, id[0] == 'a' ? 500 : 100), 0, id[0] == 'c' ? 50.0 : -20.0));
        }
        // With a millisecond of jitter, drift takes a while to pin down.
        sim.runFor(30 * SEC);

        Node & master = *sim.node("a");
        std::vector<EventId> ids;
        for (int i = 0; i < 60; i++) {
            ids.push_back(master.scheduleEvent("tick", ofToString(i)));
            sim.runFor(200 * MS);
        }
        sim.runFor(2 * SEC);

        bool once = true;
        int missing = 0;
        for (auto & id : ids) {
            for (auto n : {"a", "b", "c", "d"}) {
                int c = rec.count(id, n);
                if (c > 1) once = false;
                if (c == 0) missing++;
            }
        }
        ofxTest(once, "no event fires twice, despite resends");
        ofxTestEq(missing, 0, "5% loss: every event reaches every node");
        ofLogNotice("tests") << "events: worst spread in firing " << ms(rec.worstSpread());
        ofxTestLt(rec.worstSpread(), 1 * MS, "events fire within 1 ms of each other");
    }

    void testFollowerRequests() {
        Simulation sim(6);
        Simulation::Link lossy;
        lossy.delayUs = 500;
        lossy.jitterUs = 200;
        lossy.loss = 0.2;
        sim.setDefaultLink(lossy);
        Recorder rec;
        rec.watch(sim, sim.addNode(nodeConfig("master", 500)));
        rec.watch(sim, sim.addNode(nodeConfig("f1", 100), 3 * SEC, 30.0));
        rec.watch(sim, sim.addNode(nodeConfig("f2", 100), 9 * SEC, -30.0));
        sim.runFor(10 * SEC);

        std::vector<EventId> ids;
        for (int i = 0; i < 20; i++) {
            ids.push_back(sim.node(i % 2 ? "f1" : "f2")->scheduleEvent("request", "", 300 * MS));
            sim.runFor(100 * MS);
        }
        sim.runFor(3 * SEC);

        int fired = 0;
        bool once = true;
        for (auto & id : ids) {
            for (auto n : {"master", "f1", "f2"}) {
                int c = rec.count(id, n);
                fired += c > 0;
                once = once && c <= 1;
            }
        }
        ofxTest(once, "requested events fire once");
        ofxTestEq(fired, 60, "20% loss: every request reaches the master and every node");
        ofxTestLt(rec.worstSpread(), 1 * MS, "requested events fire together");
    }

    void testDeduplication() {
        // Two masters in separate halves of a split, 10 s apart in shared time.
        Simulation sim(7);
        Recorder rec;
        rec.watch(sim, sim.addNode(nodeConfig("a", 200), 0));
        Node & b = sim.addNode(nodeConfig("b", 100), 10 * SEC);
        rec.watch(sim, b);
        sim.partition({"a"});
        sim.runFor(5 * SEC);
        ofxTest(sim.node("a")->isMaster() && b.isMaster(), "each side of a split elects a master");

        EventId id = b.scheduleEvent("once");
        sim.runFor(1 * SEC);
        ofxTestEq(rec.count(id, "b"), 1, "b fires its event");

        int steps = 0, conflicts = 0;
        int64_t step = 0;
        auto listener = b.onDiscontinuity.newListener([&](const Discontinuity & d) {
            if (d.reason == Discontinuity::Reason::ClockStep) {
                steps++;
                step = d.stepUs;
            } else {
                conflicts++;
            }
        });

        // Heal: equal terms, a has the higher priority, so b steps down and
        // re-locks, stepping its shared time back about 10 s.
        sim.heal();
        sim.runFor(5 * SEC);
        b.update();
        ofxTestEq(b.getMasterId(), "a", "the higher priority wins the merge");
        ofxTestEq(conflicts, 1, "the loser reports the conflict");
        ofxTestEq(steps, 1, "and the step");
        ofxTestLt(std::llabs(step + 10 * SEC), 5 * MS, "shared time stepped back 10 s");

        // Its event's time comes around again; it mustn't fire twice.
        sim.runFor(12 * SEC);
        ofxTestEq(rec.count(id, "b"), 1, "a clock step never fires an event twice");

        // And a duplicate delivered directly is ignored too.
        Event copy;
        copy.id = id;
        copy.name = "once";
        copy.time = sim.sharedTimeOf("a") + 100 * MS;
        Message m;
        m.type = MessageType::Event;
        m.group = "test";
        m.nodeId = "a";
        m.term = sim.node("a")->getTerm();
        m.event = copy;
        b.receive(m, Endpoint("a", 1), b.getLocalTimeUs());
        b.receive(m, Endpoint("a", 1), b.getLocalTimeUs());
        sim.runFor(1 * SEC);
        ofxTestEq(rec.count(id, "b"), 1, "resent copies are ignored");
    }

    void testLatePolicies() {
        Simulation sim(8);
        Recorder rec;
        rec.watch(sim, sim.addNode(nodeConfig("m", 200)));
        Node & f = sim.addNode(nodeConfig("f", 100), 4 * SEC);
        rec.watch(sim, f);
        Simulation::Link slow;
        slow.delayUs = 100 * MS;
        slow.jitterUs = 0;
        sim.setDefaultLink(slow);
        sim.runFor(10 * SEC);

        // An explicit time is honored even if it's too soon for everyone.
        Node & m = *sim.node("m");
        EventId late = m.scheduleEventAt("late", m.getSharedTimeUs() + 20 * MS);
        EventId dropped = m.scheduleEventAt("dropped", m.getSharedTimeUs() + 20 * MS, "", LatePolicy::DropLate);
        sim.runFor(1 * SEC);

        ofxTestEq(rec.count(late, "m"), 1, "the master fires on time");
        ofxTestEq(rec.count(late, "f"), 1, "fire_late fires anyway");
        int64_t lateness = 0;
        for (auto & r : rec.fires) {
            if (r.id == late && r.node == "f") lateness = r.latenessUs;
        }
        ofxTestGt(lateness, 70 * MS, "and reports the lateness");
        ofxTestEq(rec.count(dropped, "m"), 1, "drop_late fires where it's on time");
        ofxTestEq(rec.count(dropped, "f"), 0, "drop_late drops where it's too late");
        ofxTestEq(f.getStatus().eventsDropped, (uint64_t)1, "and counts it");
    }

    void testOutputLatency() {
        Simulation sim(9);
        Recorder rec;
        Config fast = nodeConfig("fast", 200);
        Config slow = nodeConfig("slow", 100);
        slow.outputLatencyUs = 30 * MS;  // e.g. a display that shows a frame 30 ms late
        rec.watch(sim, sim.addNode(fast));
        rec.watch(sim, sim.addNode(slow, 2 * SEC));
        sim.runFor(10 * SEC);

        EventId id = sim.node("fast")->scheduleEvent("flash");
        sim.runFor(1 * SEC);
        int64_t tFast = 0, tSlow = 0;
        for (auto & r : rec.fires) {
            if (r.id != id) continue;
            (r.node == "fast" ? tFast : tSlow) = r.trueUs;
        }
        ofxTestLt(std::llabs((tFast - tSlow) - 30 * MS), 1 * MS, "output latency fires that much early");
    }

    void testTimelinesAcrossNodes() {
        Simulation sim(10);
        sim.addNode(nodeConfig("m", 300), 0, 40.0);
        sim.addNode(nodeConfig("f1", 200), 17 * SEC, -40.0);
        sim.addNode(nodeConfig("f2", 100), 3 * SEC, 90.0);
        sim.runFor(10 * SEC);

        auto positions = [&] {
            std::vector<int64_t> p;
            for (auto & id : {"m", "f1", "f2"}) p.push_back(sim.node(id)->getTimelinePositionUs("show", sim.sharedTimeOf(id)));
            return p;
        };
        auto spread = [](std::vector<int64_t> p) {
            return *std::max_element(p.begin(), p.end()) - *std::min_element(p.begin(), p.end());
        };

        sim.node("m")->play("show");
        sim.runFor(3 * SEC);
        auto p = positions();
        bool everywhere = sim.node("f1")->hasTimeline("show") && sim.node("f2")->hasTimeline("show");
        ofxTest(everywhere, "a timeline reaches every node");
        ofxTestLt(spread(p), 1 * MS, "every node computes the same position");
        ofxTestGt(p[0], 2 * SEC, "and it's playing");

        // A follower pauses it: the request goes through the master.
        sim.node("f2")->pause("show");
        sim.runFor(1 * SEC);
        auto paused = positions();
        sim.runFor(2 * SEC);
        auto still = positions();
        ofxTestLt(spread(paused), 1 * MS, "paused at the same position everywhere");
        ofxTestEq(paused[1], still[1], "and it stays paused");

        sim.node("f1")->seek("show", 60 * SEC);
        sim.node("f1")->setRate("show", 2.0);
        sim.runFor(2 * SEC);
        auto fast = positions();
        ofxTestLt(spread(fast), 1 * MS, "seek and rate changes land everywhere");
        ofxTestGt(fast[0], 60 * SEC, "at the new position");
        ofxTestEq(sim.node("f2")->getTimeline("show").rate, 2.0, "at the new rate");
    }

    void testLateJoin() {
        Simulation sim(11);
        Recorder rec;
        rec.watch(sim, sim.addNode(nodeConfig("m", 300)));
        rec.watch(sim, sim.addNode(nodeConfig("f", 100), 5 * SEC));
        sim.runFor(8 * SEC);

        Node & m = *sim.node("m");
        m.play("show");
        EventId id = m.scheduleEventAt("later", m.getSharedTimeUs() + 8 * SEC);
        sim.runFor(1 * SEC);

        Node & late = sim.addNode(nodeConfig("late", 50), 77 * SEC, 60.0);
        rec.watch(sim, late);
        sim.runFor(5 * SEC);

        ofxTest(late.hasTimeline("show"), "a late joiner gets the timelines");
        int64_t diff = late.getTimelinePositionUs("show", sim.sharedTimeOf("late")) -
                       m.getTimelinePositionUs("show", sim.sharedTimeOf("m"));
        ofxTestLt(std::llabs(diff), 1 * MS, "at the same position");
        ofxTestEq(late.getPendingEvents().size(), (size_t)1, "and the pending events");

        sim.runFor(5 * SEC);
        ofxTestEq(rec.count(id, "late"), 1, "which fire");
        ofxTestLt(rec.worstSpread(), 1 * MS, "together with everyone else");
    }

    void testFailover() {
        Simulation sim(12);
        Recorder rec;
        rec.watch(sim, sim.addNode(nodeConfig("a", 300), 0, 30.0));
        rec.watch(sim, sim.addNode(nodeConfig("b", 200), 40 * SEC, -50.0));
        rec.watch(sim, sim.addNode(nodeConfig("c", 100), 90 * SEC, 70.0));
        sim.runFor(120 * SEC);  // long enough to know the drift
        sim.node("a")->play("show");
        sim.runFor(2 * SEC);

        int steps = 0;
        auto count = [&](const Discontinuity & d) {
            if (d.reason == Discontinuity::Reason::ClockStep) steps++;
        };
        auto lb = sim.node("b")->onDiscontinuity.newListener(count);
        auto lc = sim.node("c")->onDiscontinuity.newListener(count);

        // A pending event scheduled by the master that is about to die.
        EventId pendingId = sim.node("a")->scheduleEventAt("survives", sim.sharedTimeOf("a") + 6 * SEC);
        sim.runFor(200 * MS);

        int64_t before = sim.node("b")->getTimelinePositionUs("show", sim.sharedTimeOf("b"));
        sim.kill("a");
        sim.runFor(6 * SEC);
        int64_t after = sim.node("b")->getTimelinePositionUs("show", sim.sharedTimeOf("b"));

        ofxTest(sim.node("b")->isMaster(), "the next candidate takes over");
        ofxTestEq(sim.node("b")->getTerm(), 2, "with term + 1");
        ofxTestEq(sim.node("c")->getMasterId(), "b", "the others follow it");
        ofxTestLt(std::llabs((after - before) - 6 * SEC), 1 * MS, "the timeline continues without a jump");
        sim.node("b")->update();
        sim.node("c")->update();
        ofxTestEq(steps, 0, "shared time continues without a step");
        ofxTestEq(rec.count(pendingId, "b") + rec.count(pendingId, "c"), 2, "pending events survive the master");

        EventId afterId = sim.node("b")->scheduleEvent("after");
        sim.runFor(1 * SEC);
        ofxTestEq(rec.count(afterId, "c"), 1, "the new master's events arrive");

        auto spreads = measureSpread(sim, 0, 30 * SEC, 100 * MS);
        ofxTestLt(percentile(spreads, 0.95), 1 * MS, "still in sync after failover");

        // The old master comes back as a follower: no preemption.
        sim.revive("a");
        sim.runFor(6 * SEC);
        ofxTestEq(sim.node("a")->getMasterId(), "b", "a returning node follows");
        ofxTest(!sim.node("a")->isMaster(), "without taking over");
        ofxTestLt(std::llabs(sim.node("a")->getTimelinePositionUs("show", sim.sharedTimeOf("a")) -
                             sim.node("b")->getTimelinePositionUs("show", sim.sharedTimeOf("b"))), 1 * MS,
                  "and catches up on the timeline");
    }

    void testReturningNodeWithLaterTerm() {
        // x joins, is cut off, elects itself (term 2), and is restarted
        // before the network heals. It remembers term 2; the group's master
        // is still on term 1.
        Simulation sim(15);
        sim.addNode(nodeConfig("a", 200));
        sim.addNode(nodeConfig("b", 100), 3 * SEC);
        sim.addNode(nodeConfig("x", 50), 8 * SEC);
        sim.runFor(10 * SEC);
        sim.partition({"x"});
        sim.runFor(10 * SEC);
        ofxTest(sim.node("x")->isMaster() && sim.node("a")->isMaster(), "each side has a master");
        ofxTestGt(sim.node("x")->getTerm(), sim.node("a")->getTerm(), "the cut-off node has the later term");

        sim.kill("x");
        sim.heal();
        sim.revive("x");
        sim.runFor(10 * SEC);
        ofxTestEq(sim.node("x")->getMasterId(), "a", "a returning node follows the master, whatever term it remembers");
        ofxTest(sim.node("a")->isMaster(), "without taking over");
    }

    void testReorderedBeacon() {
        Simulation sim(16);
        sim.addNode(nodeConfig("m", 200));
        Node & f = sim.addNode(nodeConfig("f", 100), 2 * SEC);
        sim.runFor(5 * SEC);

        // A beacon m sent before it became master, arriving late.
        Message old;
        old.type = MessageType::Beacon;
        old.group = "test";
        old.nodeId = "m";
        old.term = 0;
        old.priority = 200;
        old.port = 1;
        f.receive(old, Endpoint("m", 1), f.getLocalTimeUs());
        ofxTestEq(f.getMasterId(), "m", "a beacon overtaken on the way doesn't unseat the master");

        // The same from after it stepped down does.
        old.term = f.getTerm();
        f.receive(old, Endpoint("m", 1), f.getLocalTimeUs());
        ofxTest(f.getMasterId() != "m", "a current one saying it stepped down does");
    }

    void testSplitHeal() {
        Simulation sim(13);
        for (auto & p : std::vector<std::pair<std::string, int>>{{"a", 400}, {"b", 300}, {"c", 200}, {"d", 100}}) {
            sim.addNode(nodeConfig(p.first, p.second), p.second * SEC, (p.second - 250) / 5.0);
        }
        sim.runFor(10 * SEC);
        ofxTest(sim.node("a")->isMaster(), "a leads before the split");

        int conflicts = 0;
        auto count = [&](const Discontinuity & d) {
            if (d.reason == Discontinuity::Reason::MasterConflict) conflicts++;
        };
        auto la = sim.node("a")->onDiscontinuity.newListener(count);
        auto lb = sim.node("b")->onDiscontinuity.newListener(count);

        sim.partition({"a", "b"});
        sim.runFor(15 * SEC);
        ofxTest(sim.node("c")->isMaster(), "the other side elects its own master");
        ofxTestEq(sim.node("c")->getTerm(), 2, "with a higher term");

        sim.heal();
        sim.runFor(10 * SEC);
        for (auto n : sim.nodes()) n->update();

        bool agree = true;
        for (auto n : sim.nodes()) agree = agree && n->getMasterId() == "c";
        ofxTest(agree, "after healing, the higher term wins");
        ofxTest(!sim.node("a")->isMaster(), "the old master steps down");
        ofxTestEq(conflicts, 2, "the losing side reports the conflict");

        auto spreads = measureSpread(sim, 30 * SEC, 30 * SEC, 100 * MS);
        ofxTestLt(percentile(spreads, 0.95), 1 * MS, "one clock again");
    }

    void testLongRunDrift() {
        // 8 hours with crystals 200 ppm apart: 720 ms if uncorrected.
        Simulation sim(14);
        sim.addNode(nodeConfig("a", 300), 0, -100.0);
        sim.addNode(nodeConfig("b", 200), 12 * SEC, 40.0);
        sim.addNode(nodeConfig("c", 100), 345 * SEC, 100.0);

        uint64_t then = ofGetElapsedTimeMillis();
        auto spreads = measureSpread(sim, 5 * 60 * SEC, 8 * 3600 * SEC, 10 * SEC);
        uint64_t took = ofGetElapsedTimeMillis() - then;

        int64_t p95 = percentile(spreads, 0.95);
        ofLogNotice("tests") << "8 hours: p95 spread " << ms(p95) << ", max " << ms(percentile(spreads, 1.0))
                             << ", drift estimate " << sim.node("c")->getStatus().stats.driftPpm << " ppm"
                             << " (simulated in " << took << " ms)";
        ofxTestLt(p95, 1 * MS, "8 hours of drift: p95 spread under 1 ms");
        ofxTestLt(percentile(spreads, 1.0), 2 * MS, "8 hours of drift: never past 2 ms");
    }

    // --- real sockets and threads ---

    void testRealThreadsOverOsc() {
        Config ca = nodeConfig("real-a", 200);
        Config cb = nodeConfig("real-b", 100);
        for (Config * c : {&ca, &cb}) {
            c->discovery = DiscoveryMode::Static;
            c->peers = {"127.0.0.1:19400", "127.0.0.1:19402"};
        }
        ca.port = 19400;
        cb.port = 19402;

        Node a, b;
        OscTransport oa, ob;
        if (!oa.setup(ca) || !ob.setup(cb)) {
            ofxTest(false, "OSC ports 19400 and 19402 are free");
            return;
        }
        a.setup(ca, oa);
        b.setup(cb, ob);
        // b's crystal: 5 s off and 80 ppm fast.
        b.setLocalClock(std::make_shared<SkewedClock>(5 * SEC, 80.0));

        std::mutex fireMutex;
        std::map<EventId, std::vector<int64_t>> fired;  // CLOCK_MONOTONIC at the callback
        int64_t worstLateness = 0;
        auto record = [&](const FiredEvent & e) {
            int64_t now = monotonicUs();
            std::lock_guard<std::mutex> lock(fireMutex);
            fired[e.event.id].push_back(now);
            worstLateness = std::max(worstLateness, e.latenessUs);
        };
        auto la = a.onEventThread.newListener(record);
        auto lb = b.onEventThread.newListener(record);

        a.start();
        b.start();

        int64_t deadline = monotonicUs() + 15 * SEC;
        while (!(a.isMaster() && b.isLocked() && b.getMasterId() == "real-a") && monotonicUs() < deadline) {
            ofSleepMillis(50);
        }
        ofxTest(a.isMaster() && b.isLocked(), "two nodes lock over real UDP");

        // Let the tracking settle, then compare the two shared clocks.
        // OFXSYNCING_TRACE=1 prints how the follower's clock settles.
        if (getenv("OFXSYNCING_TRACE")) {
            for (int i = 0; i < 60; i++) {
                auto st = b.getStatus();
                int64_t sa = a.getSharedTimeUs();
                int64_t sb = b.getSharedTimeUs();
                ofLogNotice("debug") << "t=" << i * 250 << "ms diff " << (sb - sa) << " err " << st.stats.errorUs
                                     << " delay " << st.stats.delayUs << " p99 " << st.stats.delayP99Us
                                     << " drift " << st.stats.driftPpm << " slew " << st.slewRemainingUs;
                ofSleepMillis(250);
            }
        }
        ofSleepMillis(3000);
        std::vector<int64_t> diffs;
        for (int i = 0; i < 50; i++) {
            int64_t sa = a.getSharedTimeUs();
            int64_t sb = b.getSharedTimeUs();
            int64_t sa2 = a.getSharedTimeUs();
            diffs.push_back(std::llabs(sb - (sa + sa2) / 2));
            ofSleepMillis(20);
        }
        int64_t p95 = percentile(diffs, 0.95);
        ofLogNotice("tests") << "localhost UDP: p95 clock difference " << ms(p95);
        ofxTestLt(p95, 500, "localhost: shared clocks agree within 0.5 ms");

        std::vector<EventId> ids;
        for (int i = 0; i < 10; i++) {
            ids.push_back(a.scheduleEvent("real"));
            ofSleepMillis(70);
        }
        ofSleepMillis(500);

        int64_t worst = 0;
        size_t complete = 0;
        {
            std::lock_guard<std::mutex> lock(fireMutex);
            for (auto & id : ids) {
                auto & t = fired[id];
                if (t.size() == 2) {
                    complete++;
                    worst = std::max<int64_t>(worst, std::abs(t[0] - t[1]));
                }
            }
        }
        ofLogNotice("tests") << "localhost UDP: events fired " << ms(worst) << " apart at worst, "
                             << ms(worstLateness) << " late at worst";
        ofxTestEq(complete, ids.size(), "every event fires on both nodes");
        ofxTestLt(worst, 1 * MS, "real threads: events fire within 1 ms of each other");
        ofxTestLt(worstLateness, 1 * MS, "real threads: events fire within 1 ms of their time");

        b.stop();
        a.stop();
    }
};

int main() {
    ofInit();
    auto window = std::make_shared<ofAppNoWindow>();
    auto app = std::make_shared<ofApp>();
    ofRunApp(window, app);
    return ofRunMainLoop();
}
