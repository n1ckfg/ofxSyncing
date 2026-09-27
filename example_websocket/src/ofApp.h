#pragma once

#include <deque>

#include "ofMain.h"

#include "ofxSyncing.h"
#include "ofxSyncingWebSocket.h"

class ofApp: public ofBaseApp {
public:
    // args are the command line: --headless, --settings file.xml, and
    // --<setting> <value> to override anything in settings.xml.
    ofApp(const std::vector<std::string> & args, bool headless);

    void setup() override;
    void update() override;
    void draw() override;
    void exit() override;
    void keyPressed(int key) override;

private:
    void onEvent(const ofxSyncing::FiredEvent & e);
    void onDiscontinuity(const ofxSyncing::Discontinuity & d);
    void onRoleChange(const ofxSyncing::RoleChange & r);

    void scheduleTicks();
    void readCommands();
    void note(const std::string & line);
    std::string statusText();
    std::string peersText() const;
    void printHelp() const;

    std::vector<std::string> args;
    bool headless;

    // The node first, so the transports are destroyed first and stop
    // delivering before the node goes. Only one of them is set up.
    ofxSyncing::Node sync;
    ofxSyncing::WebSocketServerTransport server;
    ofxSyncing::WebSocketClientTransport client;
    ofxSyncing::Config config;

    bool isServer = true;
    std::string connectTo;
    bool useSSL = false;
    int64_t flashUs = 100000;

    int64_t nextTick = 0;
    int64_t flashAt = std::numeric_limits<int64_t>::min();
    ofColor flashColor = ofColor::white;

    std::deque<std::string> notes;
    uint64_t lastPrint = 0;
};
