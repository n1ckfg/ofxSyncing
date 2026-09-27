#pragma once

// The OSC transport: fast, local-network UDP.
//
// Header-only, so the core never pulls in ofxOsc. An app that includes this
// adds ofxOsc (which ships with openFrameworks) to its addons.make.
//
// Each message type has its own address, /ofxSyncing/<type>, and its fields
// map to typed OSC args, int64 for every time. The first four args are always
// group, node id, term and the sender's listening port: UDP replies come from
// a different port than the one a node listens on, so the reply port travels
// with the message.
//
// A message has to fit in one UDP datagram (about 64 KB), which limits how
// many pending events and timelines a SNAPSHOT can carry.

#include <map>
#include <memory>
#include <mutex>

#include "ofxOsc.h"

#include "ofxSyncing.h"

namespace ofxSyncing {

class OscTransport: public PeerListTransport {
public:
    ~OscTransport() {
        stop();
    }

    // Listens on port. bindAddress restricts it to one local address, such
    // as the configured interface's.
    bool setup(int port, const std::string & bindAddress = "0.0.0.0") {
        stop();
        listenPort = port;

        ofxOscReceiverSettings settings;
        settings.port = port;
        settings.host = bindAddress;
        settings.reuse = false;  // a second node on this port should fail, not steal half the packets
        settings.start = true;
        receiver.owner = this;
        if (!receiver.setup(settings) || !receiver.isListening()) {
            ofLogError("ofxSyncing") << "OSC can't listen on " << bindAddress << ":" << port;
            return false;
        }
        ofLogNotice("ofxSyncing") << "OSC listening on " << bindAddress << ":" << port;
        return true;
    }

    // The port from the config, bound to the configured interface's address
    // if there is one.
    bool setup(const Config & config) {
        std::string bind = "0.0.0.0";
        if (!config.interface.empty()) {
            std::string ip = net::interfaceAddress(config.interface);
            if (ip.empty()) {
                ofLogWarning("ofxSyncing") << config.interface << " has no address; listening on all interfaces";
            } else {
                bind = ip;
            }
        }
        return setup(config.port, bind);
    }

    void stop() {
        receiver.stop();
        std::lock_guard<std::mutex> lock(sendMutex);
        senders.clear();
    }

    int getPort() const { return listenPort; }

    bool send(const Endpoint & to, const Message & msg) override {
        if (to.empty() || to.port <= 0) return false;
        ofxOscMessage osc = encode(msg, listenPort);

        std::lock_guard<std::mutex> lock(sendMutex);
        auto & sender = senders[to];
        if (!sender) {
            sender = std::make_unique<ofxOscSender>();
            if (!sender->setup(to.host, to.port)) {
                senders.erase(to);
                return false;
            }
        }
        return sender->sendMessage(osc, false);
    }

    bool isLocal(const Endpoint & e) const override {
        if (e.port != listenPort) return false;
        if (e.host.rfind("127.", 0) == 0) return true;

        std::lock_guard<std::mutex> lock(localMutex);
        int64_t now = monotonicUs();
        if (now - localCheckedAt > 5000000 || localAddresses.empty()) {
            localAddresses = net::localAddresses();
            localCheckedAt = now;
        }
        return std::find(localAddresses.begin(), localAddresses.end(), e.host) != localAddresses.end();
    }

    // --- the wire format ---

    static std::string addressFor(MessageType type) {
        return std::string("/ofxSyncing/") + toString(type);
    }

    static ofxOscMessage encode(const Message & m, int replyPort) {
        ofxOscMessage o;
        o.setAddress(addressFor(m.type));
        o.addStringArg(m.group);
        o.addStringArg(m.nodeId);
        o.addInt64Arg(m.term);
        o.addInt32Arg(replyPort);

        auto addEvent = [&](const Event & e) {
            o.addStringArg(e.id.origin);
            o.addInt64Arg((int64_t)e.id.seq);
            o.addStringArg(e.name);
            o.addInt64Arg(e.time);
            o.addBlobArg(ofBuffer(e.payload.data(), e.payload.size()));
            o.addInt32Arg((int32_t)e.latePolicy);
        };
        auto addTimeline = [&](const TimelineState & t) {
            o.addStringArg(t.id);
            o.addInt64Arg((int64_t)t.version);
            o.addInt64Arg(t.t0);
            o.addInt64Arg(t.p0);
            o.addDoubleArg(t.rate);
        };

        switch (m.type) {
            case MessageType::Beacon:
                o.addInt32Arg(m.priority);
                o.addStringArg(m.address);
                o.addInt32Arg(m.port);
                o.addInt32Arg(m.isMaster ? 1 : 0);
                o.addStringArg(m.masterId);
                o.addInt64Arg(m.sharedUs);
                o.addInt64Arg(m.leadUs);
                o.addInt32Arg(m.stats.locked ? 1 : 0);
                o.addInt64Arg(m.stats.errorUs);
                o.addInt64Arg(m.stats.delayUs);
                o.addInt64Arg(m.stats.delayP99Us);
                o.addDoubleArg(m.stats.driftPpm);
                break;
            case MessageType::Ping:
                o.addInt32Arg((int32_t)m.seq);
                o.addInt64Arg(m.t0);
                o.addInt32Arg(m.wantSnapshot ? 1 : 0);
                break;
            case MessageType::Pong:
                o.addInt32Arg((int32_t)m.seq);
                o.addInt64Arg(m.t0);
                o.addInt64Arg(m.t1);
                o.addInt64Arg(m.t2);
                break;
            case MessageType::Event:
                addEvent(m.event);
                break;
            case MessageType::Ack:
                o.addStringArg(m.event.id.origin);
                o.addInt64Arg((int64_t)m.event.id.seq);
                break;
            case MessageType::EventRequest:
                addEvent(m.event);
                o.addInt64Arg(m.leadUs);
                break;
            case MessageType::Timeline:
                addTimeline(m.timeline);
                o.addInt32Arg(m.hasPosition ? 1 : 0);
                o.addInt32Arg(m.hasRate ? 1 : 0);
                o.addStringArg(m.requestId.origin);
                o.addInt64Arg((int64_t)m.requestId.seq);
                o.addInt64Arg(m.leadUs);
                break;
            case MessageType::Snapshot:
                o.addInt32Arg((int32_t)m.timelines.size());
                for (auto & t : m.timelines) addTimeline(t);
                o.addInt32Arg((int32_t)m.events.size());
                for (auto & e : m.events) addEvent(e);
                break;
        }
        return o;
    }

    // False if it isn't one of ours or is malformed.
    static bool decode(const ofxOscMessage & o, Message & m, int & replyPort) {
        const std::string & address = o.getAddress();
        const std::string prefix = "/ofxSyncing/";
        if (address.compare(0, prefix.size(), prefix) != 0) return false;
        if (!messageTypeFromString(address.substr(prefix.size()), m.type)) return false;

        size_t i = 0;
        bool ok = true;
        auto str = [&](std::string & v) {
            if (ok && i < o.getNumArgs() && o.getArgType(i) == OFXOSC_TYPE_STRING) v = o.getArgAsString(i++);
            else ok = false;
        };
        auto i32 = [&](int32_t & v) {
            if (ok && i < o.getNumArgs() && o.getArgType(i) == OFXOSC_TYPE_INT32) v = o.getArgAsInt32(i++);
            else ok = false;
        };
        auto i64 = [&](int64_t & v) {
            if (ok && i < o.getNumArgs() && o.getArgType(i) == OFXOSC_TYPE_INT64) v = o.getArgAsInt64(i++);
            else ok = false;
        };
        auto dbl = [&](double & v) {
            if (ok && i < o.getNumArgs() && o.getArgType(i) == OFXOSC_TYPE_DOUBLE) v = o.getArgAsDouble(i++);
            else ok = false;
        };
        auto blob = [&](std::string & v) {
            if (ok && i < o.getNumArgs() && o.getArgType(i) == OFXOSC_TYPE_BLOB) {
                ofBuffer b = o.getArgAsBlob(i++);
                v.assign(b.getData(), b.size());
            } else {
                ok = false;
            }
        };
        auto flag = [&](bool & v) {
            int32_t x = 0;
            i32(x);
            v = x != 0;
        };
        auto id = [&](EventId & v) {
            int64_t seq = 0;
            str(v.origin);
            i64(seq);
            v.seq = (uint64_t)seq;
        };
        auto event = [&](Event & e) {
            int32_t policy = 0;
            id(e.id);
            str(e.name);
            i64(e.time);
            blob(e.payload);
            i32(policy);
            e.latePolicy = policy == (int32_t)LatePolicy::DropLate ? LatePolicy::DropLate : LatePolicy::FireLate;
        };
        auto timeline = [&](TimelineState & t) {
            int64_t version = 0;
            str(t.id);
            i64(version);
            i64(t.t0);
            i64(t.p0);
            dbl(t.rate);
            t.version = (uint64_t)version;
        };

        int32_t reply = 0;
        str(m.group);
        str(m.nodeId);
        i64(m.term);
        i32(reply);
        replyPort = reply;

        switch (m.type) {
            case MessageType::Beacon:
                i32(m.priority);
                str(m.address);
                i32(m.port);
                flag(m.isMaster);
                str(m.masterId);
                i64(m.sharedUs);
                i64(m.leadUs);
                flag(m.stats.locked);
                i64(m.stats.errorUs);
                i64(m.stats.delayUs);
                i64(m.stats.delayP99Us);
                dbl(m.stats.driftPpm);
                break;
            case MessageType::Ping: {
                int32_t seq = 0;
                i32(seq);
                m.seq = (uint32_t)seq;
                i64(m.t0);
                flag(m.wantSnapshot);
                break;
            }
            case MessageType::Pong: {
                int32_t seq = 0;
                i32(seq);
                m.seq = (uint32_t)seq;
                i64(m.t0);
                i64(m.t1);
                i64(m.t2);
                break;
            }
            case MessageType::Event:
                event(m.event);
                break;
            case MessageType::Ack:
                id(m.event.id);
                break;
            case MessageType::EventRequest:
                event(m.event);
                i64(m.leadUs);
                break;
            case MessageType::Timeline:
                timeline(m.timeline);
                flag(m.hasPosition);
                flag(m.hasRate);
                id(m.requestId);
                i64(m.leadUs);
                break;
            case MessageType::Snapshot: {
                int32_t n = 0;
                i32(n);
                for (int32_t k = 0; ok && k < n; k++) {
                    TimelineState t;
                    timeline(t);
                    m.timelines.push_back(t);
                }
                i32(n);
                for (int32_t k = 0; ok && k < n; k++) {
                    Event e;
                    event(e);
                    m.events.push_back(e);
                }
                break;
            }
        }
        return ok && !m.nodeId.empty();
    }

private:
    // Stamps each message on ofxOscReceiver's listener thread, the moment
    // oscpack hands it over, and delivers it from there.
    class Receiver: public ofxOscReceiver {
    public:
        OscTransport * owner = nullptr;

    protected:
        void ProcessMessage(const osc::ReceivedMessage & m, const osc::IpEndpointName & remote) override {
            if (owner) owner->onReceive(m, remote);
        }
    };

    void onReceive(const osc::ReceivedMessage & m, const osc::IpEndpointName & remote) {
        int64_t rx = stampReceive();

        ofxOscMessage osc;
        osc.setAddress(m.AddressPattern());
        try {
            for (auto arg = m.ArgumentsBegin(); arg != m.ArgumentsEnd(); ++arg) {
                if (arg->IsInt32()) {
                    osc.addInt32Arg(arg->AsInt32Unchecked());
                } else if (arg->IsInt64()) {
                    osc.addInt64Arg(arg->AsInt64Unchecked());
                } else if (arg->IsDouble()) {
                    osc.addDoubleArg(arg->AsDoubleUnchecked());
                } else if (arg->IsFloat()) {
                    osc.addFloatArg(arg->AsFloatUnchecked());
                } else if (arg->IsString()) {
                    osc.addStringArg(arg->AsStringUnchecked());
                } else if (arg->IsBlob()) {
                    const void * data = nullptr;
                    osc::osc_bundle_element_size_t size = 0;
                    arg->AsBlobUnchecked(data, size);
                    osc.addBlobArg(ofBuffer((const char *)data, (size_t)size));
                } else {
                    return;  // not one of ours
                }
            }
        } catch (const std::exception &) {
            return;
        }

        Message msg;
        int replyPort = 0;
        if (!decode(osc, msg, replyPort)) return;

        char host[osc::IpEndpointName::ADDRESS_STRING_LENGTH];
        remote.AddressAsString(host);
        deliver(msg, Endpoint(host, replyPort), rx);
    }

    Receiver receiver;
    int listenPort = 0;

    std::mutex sendMutex;
    std::map<Endpoint, std::unique_ptr<ofxOscSender>> senders;

    mutable std::mutex localMutex;
    mutable std::vector<std::string> localAddresses;
    mutable int64_t localCheckedAt = 0;
};

} // namespace ofxSyncing
