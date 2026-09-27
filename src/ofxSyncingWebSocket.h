#pragma once

// The WebSocket transport: TCP, for sync beyond the local network.
//
// A star: the master runs an ofxHTTP::SimpleWebSocketServer, and each
// follower connects to it with a Poco::Net::WebSocket on a thread of its own.
// Followers never become master (give them priority 0).
//
// Header-only, so the core never pulls in ofxHTTP. An app that includes this
// lists ofxHTTP and what it depends on in its addons.make:
//
//   ofxHTTP ofxIO ofxMediaType ofxNetworkUtils ofxPoco ofxSSLManager
//
// On 64-bit Pi OS, ofxPoco needs a linuxaarch64 section in its
// addon_config.mk; example_websocket/setup.sh adds it.
//
// Frames are JSON (see ofxSyncingCodec.h), one message per text frame, so a
// browser can take part. Two more frames pass only between the adapters:
//
//   {"type":"auth","token":"..."}      client to server
//   {"type":"auth_result","ok":true}   the answer
//
// A client must present the auth token before the server accepts its event
// requests and timeline changes; without one it can still sync and watch.
// And a frame the server means for one client carries "to": that client's
// node id, which the others ignore.

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <thread>

#include "ofxHTTP.h"

#include "Poco/Buffer.h"
#include "Poco/Net/HTTPClientSession.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPSClientSession.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/WebSocket.h"

#include "ofxSyncing.h"

namespace ofxSyncing {

namespace detail {

// Compares without leaking how much of the token matched through timing.
inline bool sameToken(const std::string & a, const std::string & b) {
    unsigned char diff = a.size() == b.size() ? 0 : 1;
    for (size_t i = 0; i < a.size(); i++) {
        diff |= (unsigned char)(a[i] ^ (i < b.size() ? b[i] : 0));
    }
    return diff == 0;
}

inline std::string jsonString(const ofJson & j, const char * key) {
    auto it = j.find(key);
    return it != j.end() && it->is_string() ? it->get<std::string>() : std::string();
}

} // namespace detail

// --- the master's side -------------------------------------------------------

class WebSocketServerTransport: public Transport {
public:
    struct Settings {
        int port = 9400;

        // Empty means anyone may schedule events and change timelines.
        std::string authToken;

        // Poco drops a frame that won't fit, so this has to hold the
        // largest SNAPSHOT. ofxHTTP's default is 8 KB.
        size_t bufferSize = 256 * 1024;

        // ofxHTTP's connection loop blocks this long twice a turn, and a
        // PING that arrives in the wrong half waits out the rest before it's
        // read. That wait counts against the outbound leg only, so it skews
        // offsets as well as delays. ofxHTTP's default is 10 ms; 1 ms costs a
        // little CPU per connection.
        int pollTimeoutMs = 1;

        // wss://. Call ofSSLManager::initializeServer() with a certificate
        // first (or put ssl/certificate.pem and ssl/privateKey.pem in
        // bin/data). Browsers on https pages refuse plain ws://.
        bool useSSL = false;
    };

    ~WebSocketServerTransport() {
        stop();
    }

    // Also serves bin/data/DocumentRoot over HTTP on the same port, for the
    // browser client.
    bool setup(const Settings & s) {
        stop();
        settings = s;

        // Set up the same way as the Pinopticon apps' setupWsServer, with the
        // frame buffer raised.
        ofxHTTP::SimpleWebSocketServerSettings ws;
        ws.setPort((uint16_t)settings.port);
        ws.setUseSSL(settings.useSSL);
        ws.webSocketRouteSettings.setBufferSize(settings.bufferSize);
        ws.webSocketRouteSettings.setPollTimeout(Poco::Timespan(0, settings.pollTimeoutMs * 1000));
        server.setup(ws);
        server.webSocketRoute().registerWebSocketEvents(this);
        registered = true;
        server.start();

        if (!server.isRunning()) {
            ofLogError("ofxSyncing") << "WebSocket server couldn't start on port " << settings.port;
            return false;
        }
        ofLogNotice("ofxSyncing") << "WebSocket server on port " << settings.port
                                  << (settings.authToken.empty() ? ", no auth token" : ", auth token required to schedule");
        return true;
    }

    void stop() {
        if (registered) {
            server.webSocketRoute().unregisterWebSocketEvents(this);
            registered = false;
        }
        if (server.isRunning()) server.stop();
        std::lock_guard<std::mutex> lock(mutex);
        clients.clear();
    }

    size_t getNumConnections() {
        return server.webSocketRoute().numConnections();
    }

    bool send(const Endpoint & to, const Message & msg) override {
        // A reply from inside the sender's own frame callback (a PONG, a
        // SNAPSHOT) goes straight to its connection, which sends it the
        // moment the callback returns.
        if (current && currentKey == to.toString()) {
            return current->sendFrame(ofxHTTP::WebSocketFrame(encodeJson(msg)));
        }

        // Anywhere else the connection may be closing on another thread, so
        // address the client by node id and let the route broadcast it.
        std::string nodeId;
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto it = clients.find(to.toString());
            if (it == clients.end() || it->second.nodeId.empty()) return false;
            nodeId = it->second.nodeId;
        }
        ofJson j = toJson(msg);
        j["to"] = nodeId;
        server.webSocketRoute().broadcast(ofxHTTP::WebSocketFrame(j.dump(-1, ' ', false, ofJson::error_handler_t::replace)));
        return true;
    }

    void broadcast(const Message & msg) override {
        server.webSocketRoute().broadcast(ofxHTTP::WebSocketFrame(encodeJson(msg)));
    }

    // --- ofxHTTP's events, on each connection's own thread ---

    void onWebSocketOpenEvent(ofxHTTP::WebSocketOpenEventArgs & evt) {
        // A new connection starts unauthorized, even from an address and
        // port an authorized one used before.
        std::string key = keyOf(evt.connection());
        std::lock_guard<std::mutex> lock(mutex);
        clients[key] = Client();
        ofLogNotice("ofxSyncing") << "WebSocket client connected from " << key;
    }

    void onWebSocketCloseEvent(ofxHTTP::WebSocketCloseEventArgs & evt) {
        forget(evt.connection());
    }

    void onWebSocketErrorEvent(ofxHTTP::WebSocketErrorEventArgs & evt) {
        forget(evt.connection());
    }

    void onWebSocketFrameSentEvent(ofxHTTP::WebSocketFrameEventArgs &) {
    }

    void onWebSocketFrameReceivedEvent(ofxHTTP::WebSocketFrameEventArgs & evt) {
        int64_t rx = stampReceive();

        ofxHTTP::WebSocketConnection & connection = evt.connection();
        Poco::Net::SocketAddress address = connection.clientAddress();
        Endpoint from(address.host().toString(), address.port());
        std::string key = from.toString();

        // The payload, as bytes: WebSocketFrame::toString() describes the
        // frame's flags instead.
        const ofxHTTP::WebSocketFrame & frame = evt.frame();
        ofJson j = ofJson::parse(std::string(frame.getCharPtr(), frame.size()), nullptr, false);
        if (j.is_discarded() || !j.is_object()) return;

        if (detail::jsonString(j, "type") == "auth") {
            bool ok = settings.authToken.empty() || detail::sameToken(detail::jsonString(j, "token"), settings.authToken);
            {
                std::lock_guard<std::mutex> lock(mutex);
                clients[key].authorized = ok;
            }
            ofLogNotice("ofxSyncing") << key << (ok ? " authorized" : " presented a wrong auth token");
            connection.sendFrame(ofxHTTP::WebSocketFrame(ofJson{{"type", "auth_result"}, {"ok", ok}}.dump()));
            return;
        }

        Message msg;
        try {
            if (!fromJson(j, msg)) return;
        } catch (const std::exception &) {
            return;
        }

        // Clients follow. Nothing one sends may claim otherwise, and only an
        // authorized one may change what everyone does.
        bool authorized;
        {
            std::lock_guard<std::mutex> lock(mutex);
            Client & client = clients[key];
            client.nodeId = msg.nodeId;
            authorized = client.authorized || settings.authToken.empty();
        }
        switch (msg.type) {
            case MessageType::Beacon:
                if (msg.isMaster) return;
                break;
            case MessageType::Ping:
            case MessageType::Ack:
                break;
            case MessageType::EventRequest:
                if (!authorized) return;
                break;
            case MessageType::Timeline:
                if (msg.timeline.version != 0 || !authorized) return;
                break;
            default:
                return;  // PONG, EVENT and SNAPSHOT come only from a master
        }

        current = &connection;
        currentKey = key;
        deliver(msg, from, rx);
        current = nullptr;
        currentKey.clear();
    }

private:
    struct Client {
        std::string nodeId;
        bool authorized = false;
    };

    static std::string keyOf(ofxHTTP::WebSocketConnection & connection) {
        Poco::Net::SocketAddress address = connection.clientAddress();
        return Endpoint(address.host().toString(), address.port()).toString();
    }

    void forget(ofxHTTP::WebSocketConnection & connection) {
        std::string key = keyOf(connection);
        std::lock_guard<std::mutex> lock(mutex);
        clients.erase(key);
    }

    ofxHTTP::SimpleWebSocketServer server;
    Settings settings;
    bool registered = false;

    std::mutex mutex;
    std::map<std::string, Client> clients;  // by address:port

    // The connection whose frame callback is running on this thread.
    inline static thread_local ofxHTTP::WebSocketConnection * current = nullptr;
    inline static thread_local std::string currentKey;
};

// --- a follower's side -------------------------------------------------------

class WebSocketClientTransport: public Transport {
public:
    struct Settings {
        std::string host;
        int port = 9400;
        std::string path = "/";

        // wss://. Uses ofSSLManager's default client context; for a server
        // with a real certificate, initialize it with the system's CAs.
        bool secure = false;

        // Presented as soon as it connects.
        std::string authToken;

        // The largest frame it accepts: it has to hold a SNAPSHOT.
        int maxFrameSize = 1024 * 1024;

        // Reconnect backoff: doubles from min to max while the server is away.
        int64_t minBackoffUs = 500000;
        int64_t maxBackoffUs = 10000000;
    };

    ~WebSocketClientTransport() {
        stop();
    }

    // Connects on a thread of its own, and keeps reconnecting.
    bool setup(const Settings & s) {
        stop();
        settings = s;
        server = Endpoint(settings.host, settings.port);
        running = true;
        thread = std::thread(&WebSocketClientTransport::run, this);
        return true;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(waitMutex);
            running = false;
        }
        waitCv.notify_all();
        if (thread.joinable()) thread.join();
    }

    bool isConnected() const { return connected; }

    // Whether the server accepted the auth token.
    bool isAuthorized() const { return authorized; }

    const Endpoint & getServer() const { return server; }

    // Everything goes to the server, whatever the endpoint.
    bool send(const Endpoint &, const Message & msg) override {
        return sendText(encodeJson(msg));
    }

    void broadcast(const Message & msg) override {
        sendText(encodeJson(msg));
    }

private:
    bool sendText(const std::string & text) {
        std::lock_guard<std::mutex> lock(sendMutex);
        if (!socket) return false;
        try {
            return socket->sendFrame(text.data(), (int)text.size(), Poco::Net::WebSocket::FRAME_TEXT) == (int)text.size();
        } catch (const std::exception &) {
            return false;
        }
    }

    void run() {
        int64_t backoff = settings.minBackoffUs;
        bool warned = false;

        while (running) {
            try {
                std::unique_ptr<Poco::Net::HTTPClientSession> session;
                if (settings.secure) {
                    session = std::make_unique<Poco::Net::HTTPSClientSession>(settings.host, (Poco::UInt16)settings.port,
                                                                              ofSSLManager::getDefaultClientContext());
                } else {
                    session = std::make_unique<Poco::Net::HTTPClientSession>(settings.host, (Poco::UInt16)settings.port);
                }
                session->setTimeout(Poco::Timespan(5, 0));

                Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET, settings.path, Poco::Net::HTTPMessage::HTTP_1_1);
                Poco::Net::HTTPResponse response;
                auto ws = std::make_unique<Poco::Net::WebSocket>(*session, request, response);

                // Nagle would hold a PING back until the last frame is
                // acked, which is exactly the delay sync can't have.
                ws->setNoDelay(true);
                ws->setReceiveTimeout(Poco::Timespan(10, 0));
                ws->setMaxPayloadSize(settings.maxFrameSize);

                {
                    std::lock_guard<std::mutex> lock(sendMutex);
                    socket = std::move(ws);
                }
                connected = true;
                backoff = settings.minBackoffUs;
                warned = false;
                ofLogNotice("ofxSyncing") << "connected to " << server.toString();

                if (!settings.authToken.empty()) {
                    sendText(ofJson{{"type", "auth"}, {"token", settings.authToken}}.dump());
                }

                receive();
            } catch (const Poco::Exception & e) {
                if (!warned) ofLogWarning("ofxSyncing") << "WebSocket to " << server.toString() << ": " << e.displayText();
                warned = true;
            } catch (const std::exception & e) {
                if (!warned) ofLogWarning("ofxSyncing") << "WebSocket to " << server.toString() << ": " << e.what();
                warned = true;
            }

            if (connected) ofLogWarning("ofxSyncing") << "disconnected from " << server.toString();
            connected = false;
            authorized = false;
            {
                std::lock_guard<std::mutex> lock(sendMutex);
                socket.reset();
            }

            std::unique_lock<std::mutex> lock(waitMutex);
            waitCv.wait_for(lock, std::chrono::microseconds(backoff), [this] { return !running; });
            backoff = std::min(backoff * 2, settings.maxBackoffUs);
        }
    }

    void receive() {
        // Only this thread replaces the socket, so it can read it unlocked.
        Poco::Net::WebSocket & ws = *socket;
        Poco::Buffer<char> buffer(0);

        while (running) {
            // Wait here rather than in receiveFrame(), so a timeout never
            // lands in the middle of a frame.
            if (!ws.poll(Poco::Timespan(0, 200000), Poco::Net::Socket::SELECT_READ)) continue;

            int flags = 0;
            buffer.resize(0);
            int n = ws.receiveFrame(buffer, flags);
            int64_t rx = stampReceive();

            int op = flags & Poco::Net::WebSocket::FRAME_OP_BITMASK;
            if (op == Poco::Net::WebSocket::FRAME_OP_CLOSE) return;
            if (n <= 0 && flags == 0) return;  // the server went away
            if (op == Poco::Net::WebSocket::FRAME_OP_PING) {
                std::lock_guard<std::mutex> lock(sendMutex);
                ws.sendFrame(buffer.begin(), n, (int)Poco::Net::WebSocket::FRAME_FLAG_FIN | (int)Poco::Net::WebSocket::FRAME_OP_PONG);
                continue;
            }
            if (op != Poco::Net::WebSocket::FRAME_OP_TEXT || n <= 0) continue;

            handle(std::string(buffer.begin(), (size_t)n), rx);
        }
    }

    void handle(const std::string & text, int64_t rx) {
        ofJson j = ofJson::parse(text, nullptr, false);
        if (j.is_discarded() || !j.is_object()) return;

        Node * n = node;
        std::string to = detail::jsonString(j, "to");
        if (!to.empty() && (!n || to != n->getNodeId())) return;

        if (detail::jsonString(j, "type") == "auth_result") {
            auto ok = j.find("ok");
            authorized = ok != j.end() && ok->is_boolean() && ok->get<bool>();
            if (authorized) ofLogNotice("ofxSyncing") << "authorized by " << server.toString();
            else ofLogError("ofxSyncing") << server.toString() << " refused the auth token";
            return;
        }

        Message msg;
        try {
            if (!fromJson(j, msg)) return;
        } catch (const std::exception &) {
            return;
        }
        deliver(msg, server, rx);
    }

    Settings settings;
    Endpoint server;

    std::thread thread;
    std::atomic<bool> running{false};
    std::mutex waitMutex;
    std::condition_variable waitCv;

    std::mutex sendMutex;
    std::unique_ptr<Poco::Net::WebSocket> socket;
    std::atomic<bool> connected{false};
    std::atomic<bool> authorized{false};
};

} // namespace ofxSyncing
