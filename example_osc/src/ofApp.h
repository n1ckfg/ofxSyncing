#pragma once

#include <deque>

#include "ofMain.h"

#include "ofxSyncing.h"
#include "ofxSyncingOsc.h"

#include "GpioPin.h"

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
    void onEvent(const ofxSyncing::FiredEvent & e);        // main thread
    void onEventThread(const ofxSyncing::FiredEvent & e);  // scheduler thread
    void onDiscontinuity(const ofxSyncing::Discontinuity & d);
    void onRoleChange(const ofxSyncing::RoleChange & r);

    void scheduleTicks();
    void readCommands();
    void note(const std::string & line);
    std::string statusText() const;
    std::string peersText() const;
    void printHelp() const;

    std::vector<std::string> args;
    bool headless;

    // The node first, so the transport is destroyed first and stops
    // delivering before the node goes.
    ofxSyncing::Node sync;
    ofxSyncing::OscTransport osc;
    ofxSyncing::Config config;

    GpioPin gpio;
    int64_t gpioPulseUs = 1000;
    int64_t flashUs = 100000;

    // The master schedules a tick on every whole second of shared time.
    int64_t nextTick = 0;

    // The latest flash's shared time and color. draw() lights the screen
    // from shared time, not from when the event reached update().
    std::atomic<int64_t> flashAt{std::numeric_limits<int64_t>::min()};
    ofColor flashColor = ofColor::white;

    std::deque<std::string> notes;
    uint64_t lastPrint = 0;
};
