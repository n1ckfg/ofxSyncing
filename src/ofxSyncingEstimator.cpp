#include "ofxSyncingEstimator.h"

#include <cmath>

namespace ofxSyncing {

namespace {

int64_t median(std::vector<int64_t> values) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    size_t n = values.size();
    if (n % 2 == 1) return values[n / 2];
    // Average the middle two without overflowing.
    int64_t a = values[n / 2 - 1];
    int64_t b = values[n / 2];
    return a + (b - a) / 2;
}

// Nearest-rank percentile.
int64_t percentile(std::vector<int64_t> values, double p) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    size_t rank = (size_t)std::ceil(p * (double)values.size());
    rank = std::clamp<size_t>(rank, 1, values.size());
    return values[rank - 1];
}

} // namespace

void ClockEstimator::setWindowSize(int n) {
    windowSize = (size_t)std::max(n, 4);
    while (window.size() > windowSize) window.pop_front();
}

void ClockEstimator::reset(bool keepSkew) {
    window.clear();
    history.clear();
    if (!keepSkew) {
        skew = 0.0;
    }
    skewValid = false;
}

void ClockEstimator::addSample(const ClockSample & sample) {
    window.push_back(sample);
    while (window.size() > windowSize) window.pop_front();

    history.push_back(sample);
    while (history.size() > HISTORY_SIZE) history.pop_front();

    fitSkew();
}

namespace {

// Least squares of offset over local time. Returns false if there's no
// spread in time; slopeError is the standard error of the slope.
bool fitLine(const std::vector<const ClockSample *> & points, double & slope, double & slopeError) {
    size_t n = points.size();
    if (n < 3) return false;

    // Centered on the first point, to keep the doubles well-conditioned.
    int64_t x0 = points.front()->localTime();
    int64_t y0 = points.front()->offset();
    double mx = 0.0, my = 0.0;
    for (auto p : points) {
        mx += (double)(p->localTime() - x0);
        my += (double)(p->offset() - y0);
    }
    mx /= (double)n;
    my /= (double)n;

    double sxx = 0.0, sxy = 0.0;
    for (auto p : points) {
        double dx = (double)(p->localTime() - x0) - mx;
        double dy = (double)(p->offset() - y0) - my;
        sxx += dx * dx;
        sxy += dx * dy;
    }
    if (sxx <= 0.0) return false;
    slope = sxy / sxx;

    double residuals = 0.0;
    for (auto p : points) {
        double dx = (double)(p->localTime() - x0) - mx;
        double dy = (double)(p->offset() - y0) - my;
        double r = dy - slope * dx;
        residuals += r * r;
    }
    slopeError = std::sqrt(residuals / (double)(n - 2) / sxx);
    return true;
}

} // namespace

void ClockEstimator::fitSkew() {
    // The long fit: one point per bucket of consecutive samples, the
    // lowest-delay one, over minutes. The buckets count back from the newest
    // sample, and a partial one at the old end is left out.
    std::vector<const ClockSample *> points;
    size_t n = history.size();
    for (size_t end = n; end >= BUCKET_SIZE; end -= BUCKET_SIZE) {
        const ClockSample * best = nullptr;
        for (size_t i = end - BUCKET_SIZE; i < end; i++) {
            if (!best || history[i].delay() < best->delay()) best = &history[i];
        }
        points.push_back(best);
    }
    std::reverse(points.begin(), points.end());

    double slope = 0.0, slopeError = 0.0;
    if (points.size() >= 3 && points.back()->localTime() - points.front()->localTime() >= MIN_SKEW_SPAN_US &&
        fitLine(points, slope, slopeError)) {
        skew = std::clamp(slope, -MAX_SKEW, MAX_SKEW);
        skewValid = true;
        return;
    }

    // Until there's that much history, a provisional fit over the window,
    // if it's tight. On a wired network it is within seconds; on WiFi it
    // usually isn't, and the samples' ages count against them instead. Only
    // outliers are left out: delay shifts with load (an idle Pi answers
    // slower than one busy with a burst), and both kinds of sample are fine.
    if (window.size() < 5) return;
    std::vector<int64_t> delays;
    for (auto & s : window) delays.push_back(s.delay());
    int64_t typical = median(delays);

    std::vector<const ClockSample *> clean;
    for (auto & s : window) {
        if (s.delay() <= typical * 2 + 50) clean.push_back(&s);
    }
    if (clean.size() < 5 || clean.back()->localTime() - clean.front()->localTime() < MIN_PROVISIONAL_SPAN_US) return;

    if (fitLine(clean, slope, slopeError) && slopeError < MAX_PROVISIONAL_ERROR) {
        skew = std::clamp(slope, -MAX_SKEW, MAX_SKEW);
        skewValid = true;
    }
}

ClockEstimator::Estimate ClockEstimator::estimate(int64_t nowLocal) const {
    Estimate est;
    est.localUs = nowLocal;
    est.skew = skew;
    est.numSamples = (int)window.size();
    if (window.empty()) return est;

    double drift = skewValid ? RESIDUAL_DRIFT : UNKNOWN_DRIFT;
    auto bound = [&](const ClockSample & s) {
        double age = (double)std::max<int64_t>(0, nowLocal - s.localTime());
        return (double)s.delay() / 2.0 + age * drift;
    };

    std::vector<const ClockSample *> ranked;
    for (auto & s : window) ranked.push_back(&s);
    std::sort(ranked.begin(), ranked.end(), [&](const ClockSample * a, const ClockSample * b) {
        return bound(*a) < bound(*b);
    });

    // Once drift is known, the best sample and any others nearly as good:
    // averaging a few takes out processing jitter that the delay doesn't
    // show. Before, older samples would each drag the estimate back by
    // however much the clock has drifted since, so only the best.
    double best = bound(*ranked.front());
    double limit = skewValid ? best * 2.0 + 10.0 : best + 5.0;
    size_t most = std::max<size_t>(1, (window.size() + 3) / 4);

    std::vector<int64_t> offsets;
    for (auto s : ranked) {
        if (offsets.size() >= most || bound(*s) > limit) break;
        double carried = skew * (double)(nowLocal - s->localTime());
        offsets.push_back(s->offset() + (int64_t)std::llround(carried));
    }

    est.valid = true;
    est.offsetUs = median(offsets);
    est.errorUs = (int64_t)std::llround(best);
    est.minDelayUs = ranked.front()->delay();

    std::vector<int64_t> delays;
    delays.reserve(window.size());
    for (auto & s : window) delays.push_back(s.delay());
    est.medianDelayUs = median(delays);
    est.p99DelayUs = percentile(delays, 0.99);
    return est;
}

ClockEstimator::Estimate ClockEstimator::burstEstimate(const std::vector<ClockSample> & burst, int64_t nowLocal) const {
    Estimate est;
    est.localUs = nowLocal;
    est.skew = skew;
    est.numSamples = (int)burst.size();
    if (burst.empty()) return est;

    std::vector<ClockSample> sorted = burst;
    std::sort(sorted.begin(), sorted.end(), [](const ClockSample & a, const ClockSample & b) {
        return a.delay() < b.delay();
    });

    // The lowest-delay quarter, at least one, and their median offset.
    size_t keep = std::max<size_t>(1, (sorted.size() + 3) / 4);
    std::vector<int64_t> offsets;
    for (size_t i = 0; i < keep; i++) {
        double carried = skew * (double)(nowLocal - sorted[i].localTime());
        offsets.push_back(sorted[i].offset() + (int64_t)std::llround(carried));
    }

    est.valid = true;
    est.offsetUs = median(offsets);
    est.minDelayUs = sorted.front().delay();
    est.errorUs = est.minDelayUs / 2;

    std::vector<int64_t> delays;
    for (auto & s : burst) delays.push_back(s.delay());
    est.medianDelayUs = median(delays);
    est.p99DelayUs = percentile(delays, 0.99);
    return est;
}

} // namespace ofxSyncing
