#include "ofxSyncingTimeline.h"

#include <algorithm>

namespace ofxSyncing {

namespace {

// The newest version whose t0 has passed, or nullptr.
const TimelineState * inEffect(const std::vector<TimelineState> & states, int64_t sharedUs) {
    const TimelineState * best = nullptr;
    for (auto & s : states) {
        if (s.t0 <= sharedUs && (!best || s.version > best->version)) best = &s;
    }
    return best;
}

} // namespace

bool TimelineSet::apply(const TimelineState & state, int64_t sharedNow) {
    if (state.id.empty() || state.version == 0) return false;

    auto & states = timelines[state.id];
    for (auto & s : states) {
        if (s.version == state.version) return false;
    }

    const TimelineState * current = inEffect(states, sharedNow);
    if (current && state.version < current->version) return false;

    auto at = std::lower_bound(states.begin(), states.end(), state, [](const TimelineState & a, const TimelineState & b) {
        return a.version < b.version;
    });
    states.insert(at, state);
    return true;
}

void TimelineSet::replaceAll(const std::vector<TimelineState> & states) {
    timelines.clear();
    for (auto & s : states) {
        if (s.id.empty() || s.version == 0) continue;
        timelines[s.id].push_back(s);
    }
    for (auto & t : timelines) {
        std::sort(t.second.begin(), t.second.end(), [](const TimelineState & a, const TimelineState & b) {
            return a.version < b.version;
        });
    }
}

bool TimelineSet::has(const std::string & id) const {
    auto it = timelines.find(id);
    return it != timelines.end() && !it->second.empty();
}

TimelineState TimelineSet::stateAt(const std::string & id, int64_t sharedUs) const {
    TimelineState result;
    result.id = id;

    auto it = timelines.find(id);
    if (it == timelines.end() || it->second.empty()) return result;

    if (auto s = inEffect(it->second, sharedUs)) return *s;

    // Nothing has taken effect yet: paused at the first state's position.
    const TimelineState & first = it->second.front();
    result.t0 = first.t0;
    result.p0 = first.p0;
    result.rate = 0.0;
    return result;
}

int64_t TimelineSet::positionAt(const std::string & id, int64_t sharedUs) const {
    return stateAt(id, sharedUs).positionAt(sharedUs);
}

uint64_t TimelineSet::latestVersion(const std::string & id) const {
    auto s = latest(id);
    return s ? s->version : 0;
}

const TimelineState * TimelineSet::latest(const std::string & id) const {
    auto it = timelines.find(id);
    if (it == timelines.end() || it->second.empty()) return nullptr;
    return &it->second.back();
}

void TimelineSet::prune(int64_t sharedNow) {
    for (auto & t : timelines) {
        auto & states = t.second;
        const TimelineState * current = inEffect(states, sharedNow);
        if (!current) continue;
        uint64_t floor = current->version;
        states.erase(std::remove_if(states.begin(), states.end(), [floor](const TimelineState & s) {
            return s.version < floor;
        }), states.end());
    }
}

std::vector<TimelineState> TimelineSet::all() const {
    std::vector<TimelineState> result;
    for (auto & t : timelines) {
        result.insert(result.end(), t.second.begin(), t.second.end());
    }
    return result;
}

std::vector<std::string> TimelineSet::ids() const {
    std::vector<std::string> result;
    for (auto & t : timelines) {
        if (!t.second.empty()) result.push_back(t.first);
    }
    return result;
}

} // namespace ofxSyncing
