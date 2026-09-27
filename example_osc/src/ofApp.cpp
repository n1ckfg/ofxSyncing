#include "ofApp.h"

#include <poll.h>
#include <unistd.h>

// Bonjour discovery needs libavahi-client-dev; config.make turns it on.
#ifdef OFXSYNCING_AVAHI
#include "ofxSyncingAvahi.h"
#endif

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

std::atomic<int64_t> lastDispatchLateness{0};

} // namespace

ofApp::ofApp(const std::vector<std::string> & args, bool headless):
    args(args), headless(headless) {
}

void ofApp::setup() {
    ofSetWindowTitle("ofxSyncing OSC");
    ofSetFrameRate(60);
    ofSetVerticalSync(true);
    ofBackground(0);

    std::string settingsFile = "settings.xml";
    for (size_t i = 0; i + 1 < args.size(); i++) {
        if (args[i] == "--settings") settingsFile = args[i + 1];
    }
    config.load(ofToDataPath(settingsFile));

    // The example's own settings share the file.
    std::string gpioChip = "/dev/gpiochip0";
    int gpioLine = -1;
    bool fullscreen = false;
    auto apply = [&](const std::string & key, const std::string & value) {
        if (config.set(key, value)) return true;
        if (key == "gpio_chip") gpioChip = value;
        else if (key == "gpio_line") gpioLine = ofToInt(value);
        else if (key == "gpio_pulse_us") gpioPulseUs = ofToInt(value);
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

    // --node_id pi-2 --priority 200 --port 9402 ... override the file.
    for (size_t i = 0; i < args.size(); i++) {
        if (args[i].rfind("--", 0) != 0) continue;
        std::string key = args[i].substr(2);
        std::replace(key.begin(), key.end(), '-', '_');
        if (key == "headless") continue;
        std::string value = i + 1 < args.size() ? args[++i] : "";
        if (key != "settings" && !apply(key, value)) {
            ofLogWarning("example_osc") << "no setting called " << key;
        }
    }

    if (fullscreen && !headless) ofSetFullscreen(true);

    if (gpioLine >= 0) {
        if (gpio.open(gpioChip, gpioLine)) {
            note("pulsing " + gpioChip + " line " + ofToString(gpioLine) + " on every event");
        } else {
            note("no GPIO: " + gpio.getError());
        }
    }

    osc.setup(config);

    // Discovery follows config.discovery, except for Bonjour, which has to
    // be passed in.
    std::unique_ptr<Discovery> discovery;
#ifdef OFXSYNCING_AVAHI
    if (config.discovery == DiscoveryMode::Bonjour) discovery = std::make_unique<AvahiDiscovery>();
#endif
    sync.setup(config, osc, std::move(discovery));

    ofAddListener(sync.onEvent, this, &ofApp::onEvent);
    ofAddListener(sync.onEventThread, this, &ofApp::onEventThread);
    ofAddListener(sync.onDiscontinuity, this, &ofApp::onDiscontinuity);
    ofAddListener(sync.onRoleChange, this, &ofApp::onRoleChange);

    sync.start();

    if (headless) printHelp();
}

void ofApp::update() {
    // Delivers events and notifications to the listeners, on this thread.
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
    // ahead. A node that has just become master picks up from the next one.
    int64_t now = sync.getSharedTimeUs();
    int64_t horizon = now + sync.getLeadTimeUs() + 100000;
    if (nextTick <= now) nextTick = (horizon / SECOND + 1) * SECOND;
    while (nextTick <= horizon) {
        sync.scheduleEventAt("tick", nextTick);
        nextTick += SECOND;
    }
}

void ofApp::onEvent(const FiredEvent & e) {
    // Light the screen from the event's shared time rather than from now:
    // draw() then turns it on in the first frame at or after that time, on
    // every node alike, however late update() got the event.
    if (e.event.name == "tick") {
        flashColor = ofColor::white;
        flashAt = e.event.time;
    } else if (e.event.name == "flash") {
        auto rgb = ofSplitString(e.event.payload, ",");
        flashColor = rgb.size() == 3 ? ofColor(ofToInt(rgb[0]), ofToInt(rgb[1]), ofToInt(rgb[2])) : ofColor::red;
        flashAt = e.event.time;
        note("flash from " + e.event.id.origin + " at " + clockString(e.event.time));
    }
}

void ofApp::onEventThread(const FiredEvent & e) {
    // The scheduler thread, the moment the event is due: the tightest timing
    // there is, for GPIO or audio. This blocks it for the pulse width.
    if (gpio.isOpen()) {
        gpio.set(true);
        sleepUntilMonotonicUs(monotonicUs() + gpioPulseUs);
        gpio.set(false);
    }
    lastDispatchLateness = e.latenessUs;
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
            // Kill this node to test failover; press again to bring it back.
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
    // Headless, keys arrive on stdin (one per character, then return).
    static bool open = true;
    if (!open) return;

    pollfd pfd{};
    pfd.fd = STDIN_FILENO;
    pfd.events = POLLIN;
    while (::poll(&pfd, 1, 0) > 0) {
        char c;
        if (read(STDIN_FILENO, &c, 1) != 1) {
            open = false;  // stdin closed, e.g. under systemd
            return;
        }
        if (c != '\n' && c != '\r') keyPressed(c);
    }
}

void ofApp::draw() {
    if (headless) return;

    // Everything below comes from shared time, so every screen agrees.
    int64_t now = sync.getOutputTimeUs();
    int64_t flash = flashAt;
    bool lit = sync.isLocked() && now >= flash && now < flash + flashUs;
    ofBackground(lit ? flashColor : ofColor(0));

    int64_t position = sync.getTimelinePositionUs(TIMELINE);
    float x = (float)(((position % BAR_PERIOD) + BAR_PERIOD) % BAR_PERIOD) / (float)BAR_PERIOD * ofGetWidth();
    ofSetColor(lit ? ofColor(0) : ofColor(255, 170, 0));
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

std::string ofApp::statusText() const {
    Status st = sync.getStatus();
    std::ostringstream s;
    s << st.nodeId << "  " << toString(st.role) << "  term " << st.term
      << "  master " << (st.masterId.empty() ? "-" : st.masterId) << "\n";
    s << "shared " << (sync.isLocked() ? clockString(st.sharedUs) : "(not locked)");
    if (st.role != Role::Master && st.role != Role::Stopped && st.stats.locked) {
        s << "  error " << msString(st.stats.errorUs) << "  delay " << msString(st.stats.delayUs)
          << " (p99 " << msString(st.stats.delayP99Us) << ")  drift " << ofToString(st.stats.driftPpm, 1) << " ppm";
    }
    s << "\nlead " << msString(st.leadUs) << "  events " << st.eventsFired << " fired, " << st.eventsDropped
      << " dropped, last " << lastDispatchLateness << " us late\n";

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
    ofRemoveListener(sync.onEventThread, this, &ofApp::onEventThread);
    ofRemoveListener(sync.onDiscontinuity, this, &ofApp::onDiscontinuity);
    ofRemoveListener(sync.onRoleChange, this, &ofApp::onRoleChange);
    sync.stop();
    gpio.close();
}
