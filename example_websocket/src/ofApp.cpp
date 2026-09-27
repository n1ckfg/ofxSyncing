#include "ofApp.h"

#include <poll.h>
#include <unistd.h>

using namespace ofxSyncing;

namespace {

const std::string TIMELINE = "bar";
const int64_t SECOND = 1000000;
const int64_t BAR_PERIOD = 4 * SECOND;

// hh:mm:ss.mmm, wrapping at a day: easy to compare across screens.
std::string clockString(int64_t us) {
    int64_t ms = (std::max<int64_t>(us, 0) / 1000) % 86400000;
    char buf[32];
    snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", (int)(ms / 3600000), (int)(ms / 60000 % 60),
             (int)(ms / 1000 % 60), (int)(ms % 1000));
    return buf;
}

std::string msString(int64_t us) {
    return ofToString((double)us / 1000.0, 2) + " ms";
}

} // namespace

ofApp::ofApp(const std::vector<std::string> & args, bool headless):
    args(args), headless(headless) {
}

void ofApp::setup() {
    ofSetWindowTitle("ofxSyncing WebSocket");
    ofSetFrameRate(60);
    ofSetVerticalSync(true);
    ofBackground(0);

    std::string settingsFile = "settings.xml";
    for (size_t i = 0; i + 1 < args.size(); i++) {
        if (args[i] == "--settings") settingsFile = args[i + 1];
    }
    config.load(ofToDataPath(settingsFile));

    bool fullscreen = false;
    auto apply = [&](const std::string & key, const std::string & value) {
        if (config.set(key, value)) return true;
        if (key == "ws_connect") connectTo = ofTrim(value);
        else if (key == "ws_ssl") useSSL = ofToInt(value) != 0 || value == "true";
        else if (key == "flash_ms") flashUs = (int64_t)(ofToDouble(value) * 1000.0);
        else if (key == "fullscreen") fullscreen = ofToInt(value) != 0 || value == "true";
        else return false;
        return true;
    };

    ofXml xml;
    if (xml.load(ofToDataPath(settingsFile))) {
        for (auto & child : xml.getChild("settings").getChildren()) {
            if (child.getName() != "peers") apply(child.getName(), child.getValue());
        }
    }
    for (size_t i = 0; i < args.size(); i++) {
        if (args[i].rfind("--", 0) != 0) continue;
        std::string key = args[i].substr(2);
        std::replace(key.begin(), key.end(), '-', '_');
        if (key == "headless") continue;
        std::string value = i + 1 < args.size() ? args[++i] : "";
        if (key != "settings" && !apply(key, value)) {
            ofLogWarning("example_websocket") << "no setting called " << key;
        }
    }

    if (fullscreen && !headless) ofSetFullscreen(true);

    // A star: followers reach only the master, so none of them could take
    // over from it. They're never master, and don't need discovery.
    isServer = connectTo.empty();
    auto discovery = std::make_unique<NoDiscovery>();

    if (isServer) {
        if (useSSL) ofSSLManager::initializeServer();

        WebSocketServerTransport::Settings s;
        s.port = config.port;
        s.authToken = config.authToken;
        s.useSSL = useSSL;
        server.setup(s);
        sync.setup(config, server, std::move(discovery));
    } else {
        config.priority = 0;
        if (useSSL) {
            // The system's CAs, so a real certificate (Let's Encrypt, say)
            // checks out.
            ofSSLManager::initializeClient(new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, "",
                                                                  Poco::Net::Context::VERIFY_RELAXED, 9, true));
        }

        Endpoint target = Endpoint::parse(connectTo, config.port);
        WebSocketClientTransport::Settings s;
        s.host = target.host;
        s.port = target.port;
        s.secure = useSSL;
        s.authToken = config.authToken;
        // Come back quickly when the master restarts: one that hears no
        // follower before it takes over starts a new timebase, and everyone
        // steps once to it.
        s.maxBackoffUs = 2000000;
        client.setup(s);
        sync.setup(config, client, std::move(discovery));
    }

    ofAddListener(sync.onEvent, this, &ofApp::onEvent);
    ofAddListener(sync.onDiscontinuity, this, &ofApp::onDiscontinuity);
    ofAddListener(sync.onRoleChange, this, &ofApp::onRoleChange);

    sync.start();

    if (isServer) {
        // Where followers and browsers can find this node.
        std::string scheme = useSSL ? "https://" : "http://";
        for (auto & ip : net::localAddresses()) {
            if (ip.rfind("127.", 0) == 0) continue;
            bool tailscale = ip.rfind("100.", 0) == 0;
            note("browser: " + scheme + ip + ":" + ofToString(config.port) + "/" + (tailscale ? "  (Tailscale)" : ""));
        }
    } else {
        note("connecting to " + (useSSL ? std::string("wss://") : std::string("ws://")) + connectTo);
    }

    if (headless) printHelp();
}

void ofApp::update() {
    sync.update();

    if (sync.isMaster()) scheduleTicks();

    if (headless) {
        readCommands();
        if (ofGetElapsedTimeMillis() - lastPrint >= 1000) {
            lastPrint = ofGetElapsedTimeMillis();
            std::cout << statusText() << peersText() << std::endl;
        }
    }
}

void ofApp::scheduleTicks() {
    // A tick on every whole second of shared time, scheduled a lead time
    // ahead. The lead grows with the slowest follower's round trip.
    int64_t now = sync.getSharedTimeUs();
    int64_t horizon = now + sync.getLeadTimeUs() + 100000;
    if (nextTick <= now) nextTick = (horizon / SECOND + 1) * SECOND;
    while (nextTick <= horizon) {
        sync.scheduleEventAt("tick", nextTick);
        nextTick += SECOND;
    }
}

void ofApp::onEvent(const FiredEvent & e) {
    if (e.event.name == "tick") {
        flashColor = ofColor::white;
        flashAt = e.event.time;
    } else if (e.event.name == "flash") {
        auto rgb = ofSplitString(e.event.payload, ",");
        flashColor = rgb.size() == 3 ? ofColor(ofToInt(rgb[0]), ofToInt(rgb[1]), ofToInt(rgb[2])) : ofColor::red;
        flashAt = e.event.time;
        note("flash from " + e.event.id.origin + ", " + ofToString(e.latenessUs) + " us after its time");
    }
}

void ofApp::onDiscontinuity(const Discontinuity & d) {
    note(std::string(toString(d.reason)) + ": shared time stepped " + msString(d.stepUs));
}

void ofApp::onRoleChange(const RoleChange & r) {
    std::string line = std::string("now ") + toString(r.role);
    if (r.role == Role::Follower && !r.masterId.empty()) line += " of " + r.masterId;
    if (r.term) line += ", term " + ofToString(r.term);
    note(line);
}

void ofApp::keyPressed(int key) {
    // A follower's requests go to the master, which only accepts them once
    // the auth token checks out.
    if (!isServer && std::string("epr0<>").find((char)key) != std::string::npos && !client.isAuthorized()) {
        note("not authorized: check auth_token");
    }

    int64_t position = sync.getTimelinePositionUs(TIMELINE);
    switch (key) {
        case ' ':
        case 'e': {
            ofColor c = ofColor::fromHsb(ofRandom(255), 200, 255);
            sync.scheduleEvent("flash", ofToString((int)c.r) + "," + ofToString((int)c.g) + "," + ofToString((int)c.b));
            break;
        }
        case 'p':
            if (sync.getTimeline(TIMELINE).isPlaying()) sync.pause(TIMELINE);
            else sync.play(TIMELINE);
            break;
        case OF_KEY_LEFT:
        case '<':
            sync.seek(TIMELINE, std::max<int64_t>(0, position - SECOND));
            break;
        case OF_KEY_RIGHT:
        case '>':
            sync.seek(TIMELINE, position + SECOND);
            break;
        case '0':
            sync.seek(TIMELINE, 0);
            break;
        case 'r': {
            double rate = sync.getTimeline(TIMELINE).rate;
            sync.setRate(TIMELINE, rate == 1.0 ? 2.0 : rate == 2.0 ? 0.5 : 1.0);
            break;
        }
        case 'k':
            if (sync.isRunning()) {
                sync.stop();
                note("killed: silent until k again");
            } else {
                sync.start();
                note("revived: rejoining");
            }
            break;
        case 'f':
            if (!headless) ofToggleFullscreen();
            break;
        case 'h':
        case '?':
            printHelp();
            break;
        case 'q':
            if (headless) ofExit();
            break;
    }
}

void ofApp::readCommands() {
    static bool open = true;
    if (!open) return;

    pollfd pfd{};
    pfd.fd = STDIN_FILENO;
    pfd.events = POLLIN;
    while (::poll(&pfd, 1, 0) > 0) {
        char c;
        if (read(STDIN_FILENO, &c, 1) != 1) {
            open = false;
            return;
        }
        if (c != '\n' && c != '\r') keyPressed(c);
    }
}

void ofApp::draw() {
    if (headless) return;

    int64_t now = sync.getOutputTimeUs();
    bool lit = sync.isLocked() && now >= flashAt && now < flashAt + flashUs;
    ofBackground(lit ? flashColor : ofColor(0));

    int64_t position = sync.getTimelinePositionUs(TIMELINE);
    float x = (float)(((position % BAR_PERIOD) + BAR_PERIOD) % BAR_PERIOD) / (float)BAR_PERIOD * ofGetWidth();
    ofSetColor(lit ? ofColor(0) : ofColor(0, 170, 255));
    ofDrawRectangle(x - 5, ofGetHeight() * 0.72f, 10, ofGetHeight() * 0.28f);

    ofSetColor(lit ? 0 : 255);
    ofDrawBitmapString(statusText() + "\n" + peersText(), 20, 30);

    std::string recent;
    for (auto & n : notes) recent += n + "\n";
    ofDrawBitmapString(recent, 20, ofGetHeight() * 0.45f);

    ofSetColor(lit ? 60 : 150);
    ofDrawBitmapString("space flash   p play/pause   <- -> seek   0 rewind   r rate   k kill/revive   f fullscreen",
                       20, ofGetHeight() - 16);
}

std::string ofApp::statusText() {
    Status st = sync.getStatus();
    std::ostringstream s;
    s << st.nodeId << "  " << toString(st.role) << "  term " << st.term
      << "  master " << (st.masterId.empty() ? "-" : st.masterId) << "\n";

    if (isServer) {
        s << "WebSocket server on port " << config.port << ", " << server.getNumConnections() << " connected"
          << (config.authToken.empty() ? "" : ", auth token required") << "\n";
    } else {
        s << "WebSocket to " << connectTo << ": " << (client.isConnected() ? "connected" : "connecting")
          << (client.isConnected() ? (client.isAuthorized() ? ", authorized" : ", not authorized") : "") << "\n";
    }

    s << "shared " << (sync.isLocked() ? clockString(st.sharedUs) : "(not locked)");
    if (st.role == Role::Follower && st.stats.locked) {
        s << "  error " << msString(st.stats.errorUs) << "  delay " << msString(st.stats.delayUs)
          << " (p99 " << msString(st.stats.delayP99Us) << ")  drift " << ofToString(st.stats.driftPpm, 1) << " ppm";
    }
    // Over the internet the lead scales up with the slowest round trip.
    s << "\nlead " << msString(st.leadUs) << "  events " << st.eventsFired << " fired, " << st.eventsDropped << " dropped\n";

    TimelineState tl = sync.getTimeline(TIMELINE);
    s << "timeline " << TIMELINE << ": " << (tl.isPlaying() ? "playing" : "paused") << " at "
      << ofToString((double)sync.getTimelinePositionUs(TIMELINE) / 1e6, 3) << " s";
    if (tl.isPlaying() && tl.rate != 1.0) s << " x" << tl.rate;
    s << "\n";
    return s.str();
}

std::string ofApp::peersText() const {
    std::ostringstream s;
    for (auto & p : sync.getPeers()) {
        s << "  " << p.nodeId << (p.isMaster ? " (master)" : "") << (p.live ? "" : " LOST");
        if (!p.isMaster && p.stats.locked) {
            s << "  error " << msString(p.stats.errorUs) << "  delay " << msString(p.stats.delayUs)
              << "  drift " << ofToString(p.stats.driftPpm, 1) << " ppm";
        }
        s << "\n";
    }
    return s.str();
}

void ofApp::note(const std::string & line) {
    std::string stamped = (sync.isLocked() ? clockString(sync.getSharedTimeUs()) + "  " : std::string()) + line;
    notes.push_front(stamped);
    while (notes.size() > 8) notes.pop_back();
    if (headless) std::cout << "* " << stamped << std::endl;
}

void ofApp::printHelp() const {
    std::cout << "keys (then return): e flash, p play/pause, < > seek, 0 rewind, r rate, k kill/revive, q quit"
              << std::endl;
}

void ofApp::exit() {
    ofRemoveListener(sync.onEvent, this, &ofApp::onEvent);
    ofRemoveListener(sync.onDiscontinuity, this, &ofApp::onDiscontinuity);
    ofRemoveListener(sync.onRoleChange, this, &ofApp::onRoleChange);
    sync.stop();
    client.stop();
    server.stop();
}
