#pragma once

#include <algorithm>
#include <cstdint>
#include <deque>
#include <vector>

namespace ofxSyncing {

// One NTP-style exchange: a follower's PING and the master's PONG.
struct ClockSample {
    int64_t t0 = 0;  // follower's local time, PING sent
    int64_t t1 = 0;  // master's shared time, PING received
    int64_t t2 = 0;  // master's shared time, PONG sent
    int64_t t3 = 0;  // follower's local time, PONG received

    // The master's shared time minus the follower's local time. This assumes
    // the path is equally long in each direction; WiFi often isn't, which is
    // part of why its target is looser.
    int64_t offset() const { return ((t1 - t0) + (t2 - t3)) / 2; }

    // The round trip, minus the time the master held the PING.
    int64_t delay() const { return std::max<int64_t>(0, (t3 - t0) - (t2 - t1)); }

    // When the offset was measured, on the follower's clock.
    int64_t localTime() const { return t0 + (t3 - t0) / 2; }
};

// Turns clock samples into an offset and drift estimate.
//
// WiFi and TCP delays spike, and a spike makes a sample's offset wrong by up
// to half of it, so each estimate uses only the minimum-delay samples. Drift
// comes from a linear fit of offset over minutes of history, and is used to
// carry older samples forward to now.
//
// A sample's error is bounded by half its delay, plus its age times however
// far off the drift estimate might be (as in NTP's clock filter). Until the
// drift is known that's up to 100 ppm, so a fresh sample beats an old one
// with a slightly lower delay; once it's known, delay dominates.
class ClockEstimator {
public:
    struct Estimate {
        bool valid = false;
        int64_t offsetUs = 0;      // master's shared time minus local time, at localUs
        int64_t localUs = 0;
        double skew = 0.0;         // rate of change of the offset
        int64_t errorUs = 0;       // error bound of the best sample used
        int64_t minDelayUs = 0;    // delay of the best sample used
        int64_t medianDelayUs = 0; // over the window
        int64_t p99DelayUs = 0;    // over the window
        int numSamples = 0;        // in the window
    };

    // Samples in the sliding window (default 32).
    void setWindowSize(int n);

    // Forgets every sample. keepSkew keeps the drift estimate, which is still
    // right after a failover: the new master continues the old one's rate.
    void reset(bool keepSkew);

    void addSample(const ClockSample & sample);

    // The ongoing estimate: the samples in the window with the smallest error
    // bounds (up to a quarter of it, and none worse than twice the best), each
    // carried forward to nowLocal with the drift estimate, and their median.
    Estimate estimate(int64_t nowLocal) const;

    // The initial lock: the lowest-delay quarter of a burst, median offset.
    Estimate burstEstimate(const std::vector<ClockSample> & burst, int64_t nowLocal) const;

    double getSkew() const { return skew; }
    bool hasSkew() const { return skewValid; }
    size_t size() const { return window.size(); }

    // Drift is clamped to this; a Pi crystal is within tens of ppm.
    static constexpr double MAX_SKEW = 500e-6;

    // How far off the drift may be, for aging samples: before it's measured,
    // and after.
    static constexpr double UNKNOWN_DRIFT = 100e-6;
    static constexpr double RESIDUAL_DRIFT = 5e-6;

private:
    void fitSkew();

    size_t windowSize = 32;
    std::deque<ClockSample> window;

    // Longer history for the drift fit: about 8.5 minutes at 1 Hz.
    static constexpr size_t HISTORY_SIZE = 512;
    static constexpr size_t BUCKET_SIZE = 8;
    static constexpr int64_t MIN_SKEW_SPAN_US = 20000000;
    static constexpr int64_t MIN_PROVISIONAL_SPAN_US = 5000000;
    static constexpr double MAX_PROVISIONAL_ERROR = 10e-6;
    std::deque<ClockSample> history;

    double skew = 0.0;
    bool skewValid = false;
};

} // namespace ofxSyncing
