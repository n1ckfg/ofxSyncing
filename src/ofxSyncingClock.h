#pragma once

#include <cstdint>
#include <mutex>

namespace ofxSyncing {

// CLOCK_MONOTONIC in µs, the source for ofGetElapsedTimeMicros. Never the
// system clock: systemd-timesyncd steps and slews that, and a Pi has no RTC.
int64_t monotonicUs();

// Sleeps until a CLOCK_MONOTONIC time, with clock_nanosleep(TIMER_ABSTIME).
void sleepUntilMonotonicUs(int64_t us);

// A node's local clock. The default reads CLOCK_MONOTONIC; tests substitute
// clocks that run fast or slow, or on simulated time.
class LocalClock {
public:
    virtual ~LocalClock() = default;

    virtual int64_t now() const { return monotonicUs(); }

    // The CLOCK_MONOTONIC time at which this clock reads localUs, so a thread
    // can sleep until it.
    virtual int64_t toMonotonic(int64_t localUs) const { return localUs; }
};

// CLOCK_MONOTONIC with an offset and a crystal error, for testing sync
// between nodes that share one real clock.
class SkewedClock: public LocalClock {
public:
    SkewedClock(int64_t offsetUs, double driftPpm): offsetUs(offsetUs), rate(1.0 + driftPpm * 1e-6) {}

    int64_t now() const override;
    int64_t toMonotonic(int64_t localUs) const override;

private:
    int64_t offsetUs;
    double rate;
};

// The node's estimate of shared time. The underlying clock is never
// adjusted; the node keeps an offset and a drift rate on top of it:
//
//   shared = local + offset + skew * (local - ref)
//
// After the first lock, corrections are slewed at a bounded rate, so shared
// time never runs backwards. Only an error above the step threshold jumps.
//
// Thread-safe: readers on any thread see a consistent mapping.
class SharedClock {
public:
    struct Adjustment {
        bool stepped = false;  // jumped rather than slewed (the first lock always jumps)
        int64_t stepUs = 0;    // how far shared time jumped
        int64_t errorUs = 0;   // target minus the clock, before this adjustment
    };

    void setSlewRate(int64_t usPerSecond);
    void setStepThreshold(int64_t us);

    bool isLocked() const;

    // Unlocked, with shared time equal to local time.
    void reset();

    // Steer toward the estimate
    //   shared(L) = L + offsetUs + skew * (L - nowLocal).
    // The first call locks straight onto it. After that, errors up to the
    // step threshold are slewed; larger ones, or any with forceStep, step.
    Adjustment steer(int64_t nowLocal, int64_t offsetUs, double skew, bool forceStep = false);

    int64_t toShared(int64_t localUs) const;

    // The earliest local time at which shared time reaches sharedUs.
    int64_t toLocal(int64_t sharedUs) const;

    // What's left of the current slew: how far the clock still is from the
    // estimate it's steering toward.
    int64_t remainingSlewUs(int64_t nowLocal) const;

    double getSkew() const;

private:
    int64_t toSharedLocked(int64_t localUs) const;

    mutable std::mutex mutex;
    bool locked = false;

    // shared(L) = L + offset + skew * (L - ref)
    //               + slewRate * clamp(L - ref, 0, slewDuration)
    int64_t ref = 0;
    int64_t offset = 0;
    double skew = 0.0;
    double slewRate = 0.0;     // extra µs of shared time per µs of local time while slewing
    int64_t slewDuration = 0;  // local µs the slew lasts, from ref

    double maxSlewRate = 500e-6;   // 500 µs per second
    int64_t stepThreshold = 20000; // 20 ms
};

} // namespace ofxSyncing
