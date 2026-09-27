#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace ofxSyncing {

enum class DiscoveryMode {
    Static,  // hostnames or IPs from the peers list
    Beacon,  // multicast BEACONs (the default; no dependencies)
    Bonjour  // Avahi; include ofxSyncingAvahi.h and pass an AvahiDiscovery
};

const char * toString(DiscoveryMode mode);

// Everything a node needs to know. The examples fill it from
// bin/data/settings.xml with load(); see there for a commented copy.
struct Config {
    // Installations on the same network ignore each other unless their groups match.
    std::string group = "default";

    // Empty means the hostname.
    std::string nodeId;

    // The live node with the highest priority becomes master, ties broken by
    // node id. 0 means this node never becomes master.
    int priority = 100;

    // The interface to announce and accept peers on, e.g. "eth0". A Pi with
    // eth0 and wlan0 on the same subnet could otherwise sync over WiFi
    // without anyone noticing. Empty means any.
    std::string interface;

    // Static peers, "host" or "host:port". .local names resolve through
    // libnss-mdns, and every name is re-resolved periodically.
    std::vector<std::string> peers;

    DiscoveryMode discovery = DiscoveryMode::Beacon;

    int port = 9400;                           // the transport's port
    std::string multicastGroup = "239.255.94.1";
    int multicastPort = 9401;

    double pingHz = 1.0;               // ongoing clock pings per second
    int64_t leadFloorUs = 50000;       // the least lead time an event gets
    double leadK = 3.0;                // lead = max(lead floor, leadK * p99 round trip)
    int64_t lateToleranceUs = 10000;   // how late a drop_late event may still fire
    int64_t stepThresholdUs = 20000;   // errors past this step the clock instead of slewing it
    int64_t slewRateUsPerSec = 500;    // how fast smaller errors are corrected
    int64_t outputLatencyUs = 0;       // fire this much early, to cover display or audio latency
    int64_t masterTimeoutUs = 3000000; // a master silent this long is lost
    int64_t beaconIntervalUs = 1000000;

    // WebSocket only: clients must present this before they can schedule
    // events or change timelines. Empty means no check.
    std::string authToken;

    // Log offset, delay, error and drift for this node and every peer to a
    // CSV file (relative paths are in bin/data). Empty means off.
    std::string csvLog;

    // Run the scheduler thread SCHED_FIFO at this priority (1-99), for
    // tighter dispatch. Needs an rtprio limit or CAP_SYS_NICE. 0 is off.
    int realtimePriority = 0;

    // Reads a flat <settings> file; keys that are missing keep their current
    // values. Times are in ms except output_latency_us and slew_rate_us.
    // Returns false if the file can't be read.
    bool load(const std::string & xmlPath);

    // Sets one setting by its settings.xml name, e.g. set("priority", "200"),
    // so command-line flags can override the file. peers takes a comma-
    // separated list. Returns false for a name that isn't a setting.
    bool set(const std::string & key, const std::string & value);

    // nodeId, or the hostname when that's empty.
    std::string resolvedNodeId() const;
};

} // namespace ofxSyncing
