#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "ofEvents.h"

#include "ofxSyncingClock.h"
#include "ofxSyncingConfig.h"
#include "ofxSyncingEstimator.h"
#include "ofxSyncingTimeline.h"
#include "ofxSyncingTypes.h"

namespace ofxSyncing {

class Discovery;
class Transport;

enum class Role {
    Stopped,
    Starting,  // listening for an existing master before joining an election
    Follower,  // following a master, or waiting for one to be chosen
    Master
};

const char * toString(Role role);

// An event reaching the app.
struct FiredEvent {
    Event event;

    // How long after its moment the event reached you. The moment is the
    // event's time minus output_latency_us, so output lands on time.
    int64_t latenessUs = 0;
};

// Shared time jumped, or this node's side lost a merge.
struct Discontinuity {
    enum class Reason {
        ClockStep,      // the error passed the step threshold, so shared time stepped
        MasterConflict  // two masters met (a network split healed) and this node's side lost
    };

    Reason reason = Reason::ClockStep;
    int64_t stepUs = 0;  // how far shared time jumped (ClockStep)
    std::string masterId;
    int64_t term = 0;
};

const char * toString(Discontinuity::Reason reason);

struct RoleChange {
    Role role = Role::Stopped;
    std::string masterId;
    int64_t term = 0;
};

struct PeerStatus {
    std::string nodeId;
    Endpoint route;
    int priority = 0;
    bool isMaster = false;
    std::string masterId;  // who the peer follows
    int64_t term = 0;
    bool live = false;
    int64_t lastHeardAgoUs = 0;
    SyncStats stats;       // as the peer last reported them
};

struct Status {
    Role role = Role::Stopped;
    std::string nodeId;
    std::string masterId;
    int64_t term = 0;
    int64_t sharedUs = 0;
    int64_t leadUs = 0;           // lead time events get right now
    int64_t slewRemainingUs = 0;  // correction still being slewed in
    size_t pendingEvents = 0;
    uint64_t eventsFired = 0;
    uint64_t eventsDropped = 0;   // drop_late events that were too late
    SyncStats stats;              // this node's own
};

// One node of a synchronized group.
//
// It keeps a shared clock locked to the master's, schedules events at future
// shared times so network jitter doesn't affect when they fire, and keeps
// timelines every node can read a position from.
//
// Thread-safe: transports deliver from their own threads, events fire on the
// scheduler thread, and the app calls in from the main thread.
class Node {
public:
    Node();
    ~Node();

    Node(const Node &) = delete;
    Node & operator=(const Node &) = delete;

    // Discovery follows config.discovery unless you pass a backend (Bonjour
    // needs you to). Declare the transport after the node, so it's destroyed
    // first and stops delivering.
    void setup(const Config & config, Transport & transport, std::unique_ptr<Discovery> discovery = nullptr);

    // Replaces CLOCK_MONOTONIC with another local clock. Call before start().
    void setLocalClock(std::shared_ptr<LocalClock> clock);

    // threaded runs a service thread for the protocol and a scheduler thread
    // for dispatch. Without it, call poll() yourself.
    bool start(bool threaded = true);

    // Goes silent: sends nothing, ignores what arrives, fires nothing. Start
    // again to rejoin as a returning node, which follows whoever is master.
    void stop();
    bool isRunning() const;

    // --- for transports ---

    void receive(const Message & msg, const Endpoint & from, int64_t rxLocalUs);

    // A BEACON that came through discovery rather than the transport.
    // fromHost is the sender's address.
    void receiveDiscovery(const Message & beacon, const std::string & fromHost, int64_t rxLocalUs);

    int64_t getLocalTimeUs() const;

    // --- driving it without threads (start(false)) ---

    // Does whatever protocol work and dispatch is due, and returns the local
    // time it next needs to be called.
    int64_t poll();
    int64_t poll(int64_t nowLocalUs);

    // --- main thread ---

    // Delivers queued events and notifications on this thread. Call it from
    // ofApp::update().
    void update();

    // --- time ---

    int64_t getSharedTimeUs() const;
    double getSharedTime() const;  // seconds

    // Shared time plus output_latency_us: when a frame drawn now will be seen.
    int64_t getOutputTimeUs() const;

    int64_t sharedFromLocal(int64_t localUs) const;
    int64_t localFromShared(int64_t sharedUs) const;

    // Whether shared time means anything yet.
    bool isLocked() const;

    // --- role ---

    Role getRole() const;
    bool isMaster() const;
    const std::string & getNodeId() const { return nodeId; }
    std::string getMasterId() const;
    int64_t getTerm() const;
    const Config & getConfig() const { return config; }

    // max(lead_floor, k * p99 round trip across followers). On a follower,
    // the master's figure from its beacons.
    int64_t getLeadTimeUs() const;

    Status getStatus() const;
    std::vector<PeerStatus> getPeers() const;

    // --- events ---

    // Fires on every node at one future shared time. leadUs 0 means the
    // automatic lead time. On a follower this asks the master, which assigns
    // the time. Returns the id, or an empty one if the node isn't running.
    EventId scheduleEvent(const std::string & name, const std::string & payload = "",
                          int64_t leadUs = 0, LatePolicy policy = LatePolicy::FireLate);

    // At a particular shared time.
    EventId scheduleEventAt(const std::string & name, int64_t sharedUs, const std::string & payload = "",
                            LatePolicy policy = LatePolicy::FireLate);

    std::vector<Event> getPendingEvents() const;

    // --- timelines ---

    // Each takes effect at a future shared time, like an event; leadUs 0 is
    // the automatic lead time. A timeline exists once something changes it.
    void play(const std::string & id, int64_t leadUs = 0);
    void pause(const std::string & id, int64_t leadUs = 0);
    void seek(const std::string & id, int64_t positionUs, int64_t leadUs = 0);
    void setRate(const std::string & id, double rate, int64_t leadUs = 0);

    // Any of position and rate, optionally at a particular shared time.
    void changeTimeline(const std::string & id, bool setPosition, int64_t positionUs,
                        bool setRate, double rate, int64_t leadUs = 0, int64_t atSharedUs = 0);

    bool hasTimeline(const std::string & id) const;
    std::vector<std::string> getTimelineIds() const;

    // The state in effect now.
    TimelineState getTimeline(const std::string & id) const;

    // Position at output time, for draw().
    int64_t getTimelinePositionUs(const std::string & id) const;
    double getTimelinePosition(const std::string & id) const;  // seconds

    int64_t getTimelinePositionUs(const std::string & id, int64_t sharedUs) const;

    // --- notifications ---

    // On the scheduler thread, the moment an event is due. The tightest
    // timing, for GPIO or audio. Keep listeners short and thread-safe.
    ofEvent<const FiredEvent> onEventThread;

    // From update() on the main thread. Up to a frame late; latenessUs says
    // by how much, so animations can correct for it.
    ofEvent<const FiredEvent> onEvent;

    // From update().
    ofEvent<const Discontinuity> onDiscontinuity;
    ofEvent<const RoleChange> onRoleChange;

private:
    struct Peer {
        std::string nodeId;
        Endpoint route;             // where to send to it
        bool routeFromTransport = false;
        int priority = 0;
        bool isMaster = false;
        std::string masterId;
        int64_t term = 0;
        bool heard = false;
        int64_t lastHeard = 0;      // local
        int64_t leadUs = 0;
        SyncStats stats;
        int64_t lastSnapshotSent = 0;
        bool snapshotSent = false;
    };

    struct Pending {
        Event event;
        bool fired = false;
        bool tracked = false;       // the master resends it until everyone acks
        int64_t broadcastAt = 0;
        std::set<std::string> acks;
        std::map<std::string, int64_t> lastSent;
    };

    struct OutRequest {
        Message msg;
        int64_t queuedAt = 0;
        int64_t lastSent = 0;
        bool sent = false;
    };

    struct TimelineResend {
        TimelineState state;
        EventId requestId;
        std::vector<int64_t> at;
    };

    struct Outgoing {
        enum class Kind { Send, Broadcast, Announce };
        Kind kind = Kind::Send;
        Endpoint to;
        Message msg;
    };

    struct Notification {
        bool isDiscontinuity = false;
        Discontinuity discontinuity;
        RoleChange roleChange;
    };

    enum class SyncPhase { Idle, Burst, Tracking };

    // everything below with "Locked" in its name, or called from one, runs
    // with `mutex` held

    int64_t service(int64_t now);
    int64_t dispatch(int64_t now);

    void handleLocked(const Message & msg, const Endpoint & from, int64_t now, Peer & peer);
    Peer & heardFromLocked(const Message & msg, int64_t now);
    Peer * findPeerLocked(const std::string & id);
    const Peer * findPeerLocked(const std::string & id) const;
    bool isLiveLocked(const Peer & peer, int64_t now) const;

    // roles
    void considerMasterLocked(const std::string & sender, int64_t senderTerm, int senderPriority, int64_t now);
    bool acceptFromMasterLocked(const Message & msg, const Peer & peer, int64_t now);
    void followLocked(const std::string & master, int64_t masterTerm, int64_t now, bool conflict);
    void masterLostLocked(int64_t now);
    void electLocked(int64_t now);
    void promoteLocked(int64_t now);

    // clock sync
    void syncServiceLocked(int64_t now, int64_t & next);
    void startBurstLocked(int64_t now);
    void finishBurstLocked(int64_t now);
    void trackLocked(int64_t now);
    void sendPingLocked(int64_t now);
    void onPongLocked(const Message & msg, int64_t now);
    SyncStats ownStatsLocked(int64_t now) const;
    int64_t pingIntervalUs() const;

    // events
    bool addEventLocked(const Event & event, int64_t now);
    void masterScheduleLocked(Event event, int64_t now);
    int64_t assignTimeLocked(int64_t requestedTime, int64_t leadUs, int64_t now) const;
    int64_t leadTimeLocked(int64_t now) const;
    int64_t resendIntervalLocked(const Peer & peer) const;
    void resendEventsLocked(int64_t now, int64_t & next);
    void resendRequestsLocked(int64_t now, int64_t & next);
    EventId scheduleLocked(const std::string & name, const std::string & payload, int64_t at, int64_t leadUs, LatePolicy policy);

    // timelines
    void applyTimelineChangeLocked(const std::string & id, bool hasPosition, int64_t position, bool hasRate, double rate,
                                   int64_t leadUs, int64_t at, const EventId & requestId, int64_t now);
    void resendTimelinesLocked(int64_t now, int64_t & next);

    // messages
    Message makeLocked(MessageType type) const;
    Message eventMessageLocked(const Event & event) const;
    Message timelineMessageLocked(const TimelineState & state, const EventId & requestId) const;
    Message snapshotLocked(int64_t now) const;
    void queueBeaconLocked(int64_t now);
    void queueSendLocked(const Endpoint & to, const Message & msg);
    void queueBroadcastLocked(const Message & msg);
    void flush(std::vector<Outgoing> & out);

    // notifications
    void noteRoleLocked();
    void noteDiscontinuityLocked(Discontinuity::Reason reason, int64_t stepUs);
    void kickScheduler();
    void kickService();

    void pruneLocked(int64_t now);
    void logCsvLocked(int64_t now, const std::string & kind, const std::string & who, const std::string & master,
                      int64_t peerTerm, int64_t offsetUs, const SyncStats & stats);

    void serviceLoop();
    void schedulerLoop();
    void stopThreads();

    Config config;
    std::string nodeId;
    Transport * transport = nullptr;
    std::unique_ptr<Discovery> discovery;
    std::shared_ptr<LocalClock> localClock;
    SharedClock clock;
    std::string announcedAddress;

    mutable std::mutex mutex;
    bool running = false;
    int64_t startedAt = 0;

    // role
    Role role = Role::Stopped;
    std::string masterId;
    int64_t term = 0;
    int64_t maxTermSeen = 0;
    int64_t electionSince = 0;
    std::set<std::string> skippedCandidates;
    bool everLocked = false;

    // A locked peer's shared time from its beacon: somewhere to start from
    // if this node has to become master before it ever locked.
    bool coarseValid = false;
    int64_t coarseSharedUs = 0;
    int64_t coarseLocalUs = 0;

    std::map<std::string, Peer> peers;
    std::set<Endpoint> selfEndpoints;
    std::vector<Endpoint> transportPeers;

    // clock sync
    ClockEstimator estimator;
    ClockEstimator::Estimate lastEstimate;
    SyncPhase syncPhase = SyncPhase::Idle;
    int burstSent = 0;
    int64_t burstDeadline = 0;
    std::vector<ClockSample> burstSamples;
    int64_t nextPingAt = 0;
    uint32_t pingSeq = 0;
    std::map<uint32_t, int64_t> outstandingPings;
    bool haveSnapshot = false;

    int64_t nextBeaconAt = 0;

    // events
    uint64_t eventSeq = 0;
    std::map<EventId, Pending> pending;
    std::map<EventId, int64_t> done;  // fired or dropped -> when, for de-duplication
    std::map<EventId, OutRequest> eventRequests;
    uint64_t eventsFired = 0;
    uint64_t eventsDropped = 0;

    // timelines
    TimelineSet timelines;
    std::map<EventId, OutRequest> timelineRequests;
    std::map<EventId, int64_t> handledTimelineRequests;
    std::vector<TimelineResend> timelineResends;

    std::vector<Outgoing> outbox;
    std::deque<FiredEvent> mainQueue;
    std::deque<Notification> notifications;

    std::ofstream csv;
    std::vector<std::string> csvLines;

    // threads
    std::atomic<bool> threadsRunning{false};
    std::thread serviceThread;
    std::thread schedulerThread;
    std::mutex serviceWaitMutex;
    std::condition_variable serviceCv;
    bool serviceKicked = false;
    std::mutex schedulerWaitMutex;
    std::condition_variable schedulerCv;
    bool schedulerKicked = false;
};

} // namespace ofxSyncing
