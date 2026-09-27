#include "ofxSyncingCodec.h"

#include <cmath>
#include <cstdlib>

namespace ofxSyncing {

namespace {

// Lenient readers: a missing or mistyped field keeps its default, and a
// browser's float stands in for an integer.

int64_t readInt(const ofJson & j, const char * key, int64_t def = 0) {
    auto it = j.find(key);
    if (it == j.end()) return def;
    if (it->is_number_integer()) return it->get<int64_t>();
    if (it->is_number_unsigned()) return (int64_t)it->get<uint64_t>();
    if (it->is_number_float()) return (int64_t)std::llround(it->get<double>());
    if (it->is_boolean()) return it->get<bool>() ? 1 : 0;
    if (it->is_string()) return std::strtoll(it->get<std::string>().c_str(), nullptr, 10);
    return def;
}

double readDouble(const ofJson & j, const char * key, double def = 0.0) {
    auto it = j.find(key);
    if (it == j.end() || !it->is_number()) return def;
    return it->get<double>();
}

bool readBool(const ofJson & j, const char * key, bool def = false) {
    auto it = j.find(key);
    if (it == j.end()) return def;
    if (it->is_boolean()) return it->get<bool>();
    if (it->is_number()) return it->get<double>() != 0.0;
    return def;
}

std::string readString(const ofJson & j, const char * key, const std::string & def = "") {
    auto it = j.find(key);
    if (it == j.end() || !it->is_string()) return def;
    return it->get<std::string>();
}

ofJson idToJson(const EventId & id) {
    return ofJson{{"origin", id.origin}, {"seq", id.seq}};
}

EventId idFromJson(const ofJson & j) {
    EventId id;
    if (!j.is_object()) return id;
    id.origin = readString(j, "origin");
    id.seq = (uint64_t)readInt(j, "seq");
    return id;
}

ofJson eventToJson(const Event & e) {
    return ofJson{
        {"origin", e.id.origin},
        {"seq", e.id.seq},
        {"name", e.name},
        {"time", e.time},
        {"payload", e.payload},
        {"late_policy", toString(e.latePolicy)}
    };
}

Event eventFromJson(const ofJson & j) {
    Event e;
    if (!j.is_object()) return e;
    e.id = idFromJson(j);
    e.name = readString(j, "name");
    e.time = readInt(j, "time");
    e.payload = readString(j, "payload");
    e.latePolicy = latePolicyFromString(readString(j, "late_policy"));
    return e;
}

ofJson timelineToJson(const TimelineState & t) {
    return ofJson{
        {"id", t.id},
        {"version", t.version},
        {"t0", t.t0},
        {"p0", t.p0},
        {"rate", t.rate}
    };
}

TimelineState timelineFromJson(const ofJson & j) {
    TimelineState t;
    if (!j.is_object()) return t;
    t.id = readString(j, "id");
    t.version = (uint64_t)readInt(j, "version");
    t.t0 = readInt(j, "t0");
    t.p0 = readInt(j, "p0");
    t.rate = readDouble(j, "rate");
    return t;
}

} // namespace

ofJson toJson(const Message & m) {
    ofJson j;
    j["type"] = toString(m.type);
    j["group"] = m.group;
    j["node"] = m.nodeId;
    j["term"] = m.term;

    switch (m.type) {
        case MessageType::Beacon:
            j["priority"] = m.priority;
            j["address"] = m.address;
            j["port"] = m.port;
            j["is_master"] = m.isMaster;
            j["master"] = m.masterId;
            j["shared_us"] = m.sharedUs;
            j["lead_us"] = m.leadUs;
            j["locked"] = m.stats.locked;
            j["error_us"] = m.stats.errorUs;
            j["delay_us"] = m.stats.delayUs;
            j["delay_p99_us"] = m.stats.delayP99Us;
            j["drift_ppm"] = m.stats.driftPpm;
            break;
        case MessageType::Ping:
            j["seq"] = m.seq;
            j["t0"] = m.t0;
            if (m.wantSnapshot) j["want_snapshot"] = true;
            break;
        case MessageType::Pong:
            j["seq"] = m.seq;
            j["t0"] = m.t0;
            j["t1"] = m.t1;
            j["t2"] = m.t2;
            break;
        case MessageType::Event:
            j["event"] = eventToJson(m.event);
            break;
        case MessageType::Ack:
            j["event"] = idToJson(m.event.id);
            break;
        case MessageType::EventRequest:
            j["event"] = eventToJson(m.event);
            j["lead_us"] = m.leadUs;
            break;
        case MessageType::Timeline:
            j["timeline"] = timelineToJson(m.timeline);
            if (m.hasPosition) j["has_position"] = true;
            if (m.hasRate) j["has_rate"] = true;
            if (!m.requestId.empty()) j["request"] = idToJson(m.requestId);
            if (m.leadUs) j["lead_us"] = m.leadUs;
            break;
        case MessageType::Snapshot: {
            ofJson timelines = ofJson::array();
            for (auto & t : m.timelines) timelines.push_back(timelineToJson(t));
            ofJson events = ofJson::array();
            for (auto & e : m.events) events.push_back(eventToJson(e));
            j["timelines"] = timelines;
            j["events"] = events;
            break;
        }
    }
    return j;
}

bool fromJson(const ofJson & j, Message & m) {
    if (!j.is_object()) return false;
    if (!messageTypeFromString(readString(j, "type"), m.type)) return false;

    m.group = readString(j, "group");
    m.nodeId = readString(j, "node");
    m.term = readInt(j, "term");
    if (m.nodeId.empty()) return false;

    switch (m.type) {
        case MessageType::Beacon:
            m.priority = (int)readInt(j, "priority");
            m.address = readString(j, "address");
            m.port = (int)readInt(j, "port");
            m.isMaster = readBool(j, "is_master");
            m.masterId = readString(j, "master");
            m.sharedUs = readInt(j, "shared_us");
            m.leadUs = readInt(j, "lead_us");
            m.stats.locked = readBool(j, "locked");
            m.stats.errorUs = readInt(j, "error_us");
            m.stats.delayUs = readInt(j, "delay_us");
            m.stats.delayP99Us = readInt(j, "delay_p99_us");
            m.stats.driftPpm = readDouble(j, "drift_ppm");
            break;
        case MessageType::Ping:
            m.seq = (uint32_t)readInt(j, "seq");
            m.t0 = readInt(j, "t0");
            m.wantSnapshot = readBool(j, "want_snapshot");
            break;
        case MessageType::Pong:
            m.seq = (uint32_t)readInt(j, "seq");
            m.t0 = readInt(j, "t0");
            m.t1 = readInt(j, "t1");
            m.t2 = readInt(j, "t2");
            break;
        case MessageType::Event:
        case MessageType::EventRequest:
            if (!j.contains("event")) return false;
            m.event = eventFromJson(j["event"]);
            m.leadUs = readInt(j, "lead_us");
            if (m.event.id.empty()) return false;
            break;
        case MessageType::Ack:
            if (!j.contains("event")) return false;
            m.event.id = idFromJson(j["event"]);
            if (m.event.id.empty()) return false;
            break;
        case MessageType::Timeline:
            if (!j.contains("timeline")) return false;
            m.timeline = timelineFromJson(j["timeline"]);
            m.hasPosition = readBool(j, "has_position");
            m.hasRate = readBool(j, "has_rate");
            if (j.contains("request")) m.requestId = idFromJson(j["request"]);
            m.leadUs = readInt(j, "lead_us");
            if (m.timeline.id.empty()) return false;
            break;
        case MessageType::Snapshot:
            if (j.contains("timelines") && j["timelines"].is_array()) {
                for (auto & t : j["timelines"]) m.timelines.push_back(timelineFromJson(t));
            }
            if (j.contains("events") && j["events"].is_array()) {
                for (auto & e : j["events"]) m.events.push_back(eventFromJson(e));
            }
            break;
    }
    return true;
}

std::string encodeJson(const Message & msg) {
    // A payload that isn't valid UTF-8 gets replacement characters rather
    // than an exception.
    return toJson(msg).dump(-1, ' ', false, ofJson::error_handler_t::replace);
}

bool decodeJson(const std::string & text, Message & msg) {
    ofJson j = ofJson::parse(text, nullptr, false);
    if (j.is_discarded()) return false;
    try {
        return fromJson(j, msg);
    } catch (const std::exception &) {
        return false;
    }
}

} // namespace ofxSyncing
