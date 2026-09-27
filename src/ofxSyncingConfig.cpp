#include "ofxSyncingConfig.h"

#include <cmath>

#include "ofLog.h"
#include "ofUtils.h"
#include "ofXml.h"

#include "ofxSyncingNet.h"

namespace ofxSyncing {

const char * toString(DiscoveryMode mode) {
    switch (mode) {
        case DiscoveryMode::Static: return "static";
        case DiscoveryMode::Beacon: return "beacon";
        case DiscoveryMode::Bonjour: return "bonjour";
    }
    return "unknown";
}

bool Config::load(const std::string & xmlPath) {
    ofXml xml;
    if (!xml.load(xmlPath)) {
        ofLogWarning("ofxSyncing") << "couldn't read " << xmlPath << ", using defaults";
        return false;
    }

    ofXml root = xml.getChild("settings");
    if (!root) root = xml.getFirstChild();

    for (auto & child : root.getChildren()) {
        std::string key = child.getName();
        if (key == "peers") {
            // <peers><peer>a</peer><peer>b</peer></peers>, or <peers>a, b</peers>
            std::vector<std::string> list;
            for (auto & p : child.getChildren("peer")) {
                std::string v = ofTrim(p.getValue());
                if (!v.empty()) list.push_back(v);
            }
            set(key, list.empty() ? child.getValue() : ofJoinString(list, ","));
        } else {
            // Settings for the app share the file; those are its business.
            set(key, child.getValue());
        }
    }
    return true;
}

bool Config::set(const std::string & key, const std::string & rawValue) {
    std::string value = ofTrim(rawValue);
    auto ms = [&](int64_t & us) { us = (int64_t)std::llround(ofToDouble(value) * 1000.0); };
    auto micros = [&](int64_t & us) { us = (int64_t)std::llround(ofToDouble(value)); };

    if (key == "group") group = value;
    else if (key == "node_id") nodeId = value;
    else if (key == "priority") priority = ofToInt(value);
    else if (key == "interface") interface = value;
    else if (key == "peers") peers = ofSplitString(value, ",", true, true);
    else if (key == "discovery") {
        std::string mode = ofToLower(value);
        if (mode == "static") discovery = DiscoveryMode::Static;
        else if (mode == "beacon" || mode == "multicast") discovery = DiscoveryMode::Beacon;
        else if (mode == "bonjour" || mode == "avahi") discovery = DiscoveryMode::Bonjour;
        else ofLogWarning("ofxSyncing") << "unknown discovery \"" << value << "\", keeping " << toString(discovery);
    }
    else if (key == "port") port = ofToInt(value);
    else if (key == "multicast_group") multicastGroup = value;
    else if (key == "multicast_port") multicastPort = ofToInt(value);
    else if (key == "ping_hz") pingHz = ofToDouble(value);
    else if (key == "lead_floor_ms") ms(leadFloorUs);
    else if (key == "lead_k") leadK = ofToDouble(value);
    else if (key == "late_tolerance_ms") ms(lateToleranceUs);
    else if (key == "step_threshold_ms") ms(stepThresholdUs);
    else if (key == "slew_rate_us") micros(slewRateUsPerSec);
    else if (key == "output_latency_us") micros(outputLatencyUs);
    else if (key == "master_timeout_ms") ms(masterTimeoutUs);
    else if (key == "beacon_interval_ms") ms(beaconIntervalUs);
    else if (key == "auth_token") authToken = value;
    else if (key == "csv_log") csvLog = value;
    else if (key == "realtime_priority") realtimePriority = ofToInt(value);
    else return false;
    return true;
}

std::string Config::resolvedNodeId() const {
    if (!nodeId.empty()) return nodeId;
    std::string host = net::hostName();
    return host.empty() ? "node" : host;
}

} // namespace ofxSyncing
