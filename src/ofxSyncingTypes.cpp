#include "ofxSyncingTypes.h"

#include <cctype>
#include <cmath>
#include <cstdlib>

namespace ofxSyncing {

std::string Endpoint::toString() const {
    if (host.find(':') != std::string::npos) {
        return "[" + host + "]:" + std::to_string(port);
    }
    return host + ":" + std::to_string(port);
}

Endpoint Endpoint::parse(const std::string & text, int defaultPort) {
    Endpoint e;
    e.port = defaultPort;

    std::string s = text;
    while (!s.empty() && std::isspace((unsigned char)s.front())) s.erase(s.begin());
    while (!s.empty() && std::isspace((unsigned char)s.back())) s.pop_back();

    if (!s.empty() && s.front() == '[') {
        auto close = s.find(']');
        if (close == std::string::npos) {
            e.host = s.substr(1);
            return e;
        }
        e.host = s.substr(1, close - 1);
        if (close + 1 < s.size() && s[close + 1] == ':') {
            e.port = std::atoi(s.c_str() + close + 2);
        }
        return e;
    }

    // One colon is host:port; more than one is a bare IPv6 address.
    auto colon = s.find(':');
    if (colon != std::string::npos && s.find(':', colon + 1) == std::string::npos) {
        e.host = s.substr(0, colon);
        e.port = std::atoi(s.c_str() + colon + 1);
    } else {
        e.host = s;
    }
    return e;
}

const char * toString(MessageType type) {
    switch (type) {
        case MessageType::Beacon: return "beacon";
        case MessageType::Ping: return "ping";
        case MessageType::Pong: return "pong";
        case MessageType::Event: return "event";
        case MessageType::Ack: return "ack";
        case MessageType::EventRequest: return "event_request";
        case MessageType::Timeline: return "timeline";
        case MessageType::Snapshot: return "snapshot";
    }
    return "unknown";
}

bool messageTypeFromString(const std::string & text, MessageType & type) {
    static const MessageType all[] = {
        MessageType::Beacon, MessageType::Ping, MessageType::Pong, MessageType::Event,
        MessageType::Ack, MessageType::EventRequest, MessageType::Timeline, MessageType::Snapshot
    };
    for (auto t : all) {
        if (text == toString(t)) {
            type = t;
            return true;
        }
    }
    return false;
}

const char * toString(LatePolicy policy) {
    return policy == LatePolicy::DropLate ? "drop_late" : "fire_late";
}

LatePolicy latePolicyFromString(const std::string & text) {
    return text == "drop_late" ? LatePolicy::DropLate : LatePolicy::FireLate;
}

std::string EventId::toString() const {
    return origin + "#" + std::to_string(seq);
}

int64_t TimelineState::positionAt(int64_t sharedUs) const {
    if (rate == 0.0) return p0;
    return p0 + (int64_t)std::llround((double)(sharedUs - t0) * rate);
}

} // namespace ofxSyncing
