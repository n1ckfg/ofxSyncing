#include "ofxSyncingClock.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <thread>
#include <time.h>

namespace ofxSyncing {

int64_t monotonicUs() {
    timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

void sleepUntilMonotonicUs(int64_t us) {
#ifdef __linux__
    timespec ts;
    ts.tv_sec = us / 1000000;
    ts.tv_nsec = (us % 1000000) * 1000;
    while (clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &ts, nullptr) == EINTR) {
    }
#else
    int64_t wait = us - monotonicUs();
    if (wait > 0) std::this_thread::sleep_for(std::chrono::microseconds(wait));
#endif
}

int64_t SkewedClock::now() const {
    int64_t mono = monotonicUs();
    return offsetUs + (int64_t)std::llround((double)mono * rate);
}

int64_t SkewedClock::toMonotonic(int64_t localUs) const {
    return (int64_t)std::ceil((double)(localUs - offsetUs) / rate);
}

void SharedClock::setSlewRate(int64_t usPerSecond) {
    std::lock_guard<std::mutex> lock(mutex);
    // Past 10% the clock would visibly speed up and slow down, and at 100%
    // it could stand still.
    maxSlewRate = std::clamp((double)usPerSecond * 1e-6, 1e-6, 0.1);
}

void SharedClock::setStepThreshold(int64_t us) {
    std::lock_guard<std::mutex> lock(mutex);
    stepThreshold = std::max<int64_t>(us, 0);
}

bool SharedClock::isLocked() const {
    std::lock_guard<std::mutex> lock(mutex);
    return locked;
}

void SharedClock::reset() {
    std::lock_guard<std::mutex> lock(mutex);
    locked = false;
    ref = 0;
    offset = 0;
    skew = 0.0;
    slewRate = 0.0;
    slewDuration = 0;
}

SharedClock::Adjustment SharedClock::steer(int64_t nowLocal, int64_t offsetUs, double newSkew, bool forceStep) {
    std::lock_guard<std::mutex> lock(mutex);
    Adjustment adj;

    int64_t current = toSharedLocked(nowLocal) - nowLocal;
    adj.errorUs = offsetUs - current;

    // Freeze the clock where it is now and continue from here with the new
    // rate: shared time is continuous across the change.
    ref = nowLocal;
    skew = newSkew;
    slewRate = 0.0;
    slewDuration = 0;

    if (!locked || forceStep || std::llabs(adj.errorUs) > stepThreshold) {
        offset = offsetUs;
        adj.stepped = true;
        adj.stepUs = adj.errorUs;
        locked = true;
        return adj;
    }

    offset = current;
    if (adj.errorUs != 0) {
        slewRate = adj.errorUs > 0 ? maxSlewRate : -maxSlewRate;
        slewDuration = std::max<int64_t>(1, std::llround(std::fabs((double)adj.errorUs) / maxSlewRate));
    }
    return adj;
}

int64_t SharedClock::toSharedLocked(int64_t L) const {
    double s = (double)offset + skew * (double)(L - ref);
    if (slewDuration > 0 && L > ref) {
        s += slewRate * (double)std::min<int64_t>(L - ref, slewDuration);
    }
    return L + (int64_t)std::llround(s);
}

int64_t SharedClock::toShared(int64_t localUs) const {
    std::lock_guard<std::mutex> lock(mutex);
    return toSharedLocked(localUs);
}

int64_t SharedClock::toLocal(int64_t sharedUs) const {
    std::lock_guard<std::mutex> lock(mutex);

    // The mapping is piecewise linear and always increasing: before ref,
    // during the slew, and after it. Invert the segment sharedUs falls in.
    double a = (double)sharedUs - (double)ref - (double)offset;
    double x;
    if (a <= 0.0 || slewDuration <= 0) {
        x = a / (1.0 + skew);
    } else {
        double slewEnd = (1.0 + skew + slewRate) * (double)slewDuration;
        if (a <= slewEnd) {
            x = a / (1.0 + skew + slewRate);
        } else {
            x = (a - slewRate * (double)slewDuration) / (1.0 + skew);
        }
    }

    // Rounding in toShared makes the last µs uneven; settle on the first
    // local time that actually reaches sharedUs.
    int64_t L = ref + (int64_t)std::ceil(x);
    while (toSharedLocked(L) < sharedUs) L++;
    while (toSharedLocked(L - 1) >= sharedUs) L--;
    return L;
}

int64_t SharedClock::remainingSlewUs(int64_t nowLocal) const {
    std::lock_guard<std::mutex> lock(mutex);
    if (slewDuration <= 0) return 0;
    int64_t elapsed = std::clamp<int64_t>(nowLocal - ref, 0, slewDuration);
    return (int64_t)std::llround(slewRate * (double)(slewDuration - elapsed));
}

double SharedClock::getSkew() const {
    std::lock_guard<std::mutex> lock(mutex);
    return skew;
}

} // namespace ofxSyncing
