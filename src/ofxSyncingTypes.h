#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <tuple>
#include <vector>

// Every timestamp in the addon is an int64 count of microseconds. OSC float32
// would lose precision within hours.
//
// "Local" times come from the node's own CLOCK_MONOTONIC. "Shared" times are
// on the clock the whole group agrees on, which is the master's.

namespace ofxSyncing {

// A time that never comes.
constexpr int64_t NEVER = std::numeric_limits<int64_t>::max();

// Where a transport can reach a node. host is usually an IP address, but a
// transport may use any key it can route back to (the WebSocket server uses
// the client's address, one per connection).
struct Endpoint {
    std::string host;
    int port = 0;

    Endpoint() = default;
    Endpoint(const std::string & host, int port): host(host), port(port) {}

    bool empty() const { return host.empty(); }
    std::string toString() const;

    // "host", "host:port" or "[v6addr]:port". A missing port is defaultPort.
    static Endpoint parse(const std::string & text, int defaultPort);

    bool operator==(const Endpoint & o) const { return host == o.host && port == o.port; }
    bool operator!=(const Endpoint & o) const { return !(*this == o); }
    bool operator<(const Endpoint & o) const { return std::tie(host, port) < std::tie(o.host, o.port); }
};

enum class MessageType {
    Beacon,       // discovery and master liveness
    Ping,         // clock sync request (follower to master)
    Pong,         // clock sync reply
    Event,        // scheduled event
    Ack,          // a follower confirms it received an event
    EventRequest, // a non-master node asks the master to schedule an event
    Timeline,     // timeline state change, or a request for one (version 0)
    Snapshot      // full state for a node that joins late
};

const char * toString(MessageType type);
bool messageTypeFromString(const std::string & text, MessageType & type);

// What happens to an event that can't fire on time.
enum class LatePolicy {
    FireLate, // fire immediately and report the lateness
    DropLate  // drop it when it's later than late_tolerance_ms
};

const char * toString(LatePolicy policy);
LatePolicy latePolicyFromString(const std::string & text);

// Unique across the group: the node that created the event, and a number that
// node never reuses (it starts at random, so a restarted node doesn't collide
// with ids the others still remember).
struct EventId {
    std::string origin;
    uint64_t seq = 0;

    EventId() = default;
    EventId(const std::string & origin, uint64_t seq): origin(origin), seq(seq) {}

    bool empty() const { return origin.empty(); }
    std::string toString() const;

    bool operator==(const EventId & o) const { return seq == o.seq && origin == o.origin; }
    bool operator!=(const EventId & o) const { return !(*this == o); }
    bool operator<(const EventId & o) const { return std::tie(origin, seq) < std::tie(o.origin, o.seq); }
};

struct Event {
    EventId id;
    std::string name;
    std::string payload;  // opaque to the addon; keep it text if a WebSocket client will see it
    int64_t time = 0;     // shared time it fires at
    LatePolicy latePolicy = LatePolicy::FireLate;
};

// A timeline is state, not a stream of events. Every node computes
//   position(now) = p0 + (shared_now - t0) * rate
// locally. A newer version supersedes older ones from its t0 on.
struct TimelineState {
    std::string id;
    uint64_t version = 0;
    int64_t t0 = 0;     // shared time this state takes effect
    int64_t p0 = 0;     // timeline position at t0, µs
    double rate = 0.0;  // 1 plays, 0 pauses

    int64_t positionAt(int64_t sharedUs) const;
    bool isPlaying() const { return rate != 0.0; }
};

// The sync quality a node reports about itself.
struct SyncStats {
    bool locked = false;
    int64_t errorUs = 0;     // estimated error of this node's shared clock
    int64_t delayUs = 0;     // median round-trip delay to the master
    int64_t delayP99Us = 0;  // 99th percentile round-trip delay to the master
    double driftPpm = 0.0;   // this node's crystal against the master's clock
};

// Everything the protocol sends. Which fields are meaningful depends on type;
// the rest keep their defaults. Transports serialize it however suits them.
struct Message {
    MessageType type = MessageType::Beacon;

    // on every message
    std::string group;   // installations on the same network ignore each other
    std::string nodeId;  // the sender
    int64_t term = 0;    // the master's term, as the sender knows it

    // BEACON
    int priority = 0;
    std::string address;  // the address the sender announces (may be empty)
    int port = 0;         // the sender's transport port
    bool isMaster = false;
    std::string masterId; // who the sender follows
    int64_t sharedUs = 0; // the sender's shared clock when it sent this (0 if unlocked)
    int64_t leadUs = 0;   // BEACON: the master's current lead time. EVENT_REQUEST/TIMELINE request: lead hint
    SyncStats stats;

    // PING / PONG
    uint32_t seq = 0;
    int64_t t0 = 0;       // follower's local time when it sent the PING
    int64_t t1 = 0;       // master's shared time when the PING arrived
    int64_t t2 = 0;       // master's shared time when it sent the PONG
    bool wantSnapshot = false;

    // EVENT / ACK / EVENT_REQUEST. An EVENT_REQUEST's event.time is the
    // requested shared time, or 0 for "as soon as everyone can make it".
    Event event;

    // TIMELINE. version 0 means a request to the master, which fills in the
    // rest; hasPosition/hasRate say which of p0/rate the request sets, and
    // timeline.t0 is the requested time (0 = as soon as possible). The master
    // echoes requestId in the state it broadcasts.
    TimelineState timeline;
    bool hasPosition = false;
    bool hasRate = false;
    EventId requestId;

    // SNAPSHOT
    std::vector<TimelineState> timelines;
    std::vector<Event> events;
};

} // namespace ofxSyncing
