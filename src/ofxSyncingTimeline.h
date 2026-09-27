#pragma once

#include <map>
#include <string>
#include <vector>

#include "ofxSyncingTypes.h"

namespace ofxSyncing {

// Every timeline a node knows, each as a short list of states by version.
// A change takes effect at a future shared time, so until then a timeline
// has both the state in effect and the one that's coming.
//
// At any time the state in effect is the newest version whose t0 has passed.
// A newer version therefore cancels an older one that hasn't taken effect by
// the time the newer one does.
//
// Not thread-safe; the node guards it.
class TimelineSet {
public:
    // Stores a state unless it's already known or already superseded at
    // sharedNow. Returns whether it was stored.
    bool apply(const TimelineState & state, int64_t sharedNow);

    // Replaces everything, as a SNAPSHOT does.
    void replaceAll(const std::vector<TimelineState> & states);

    bool has(const std::string & id) const;

    // The state in effect at sharedUs. Before a timeline's first state takes
    // effect it sits paused at that state's p0.
    TimelineState stateAt(const std::string & id, int64_t sharedUs) const;

    int64_t positionAt(const std::string & id, int64_t sharedUs) const;

    uint64_t latestVersion(const std::string & id) const;
    const TimelineState * latest(const std::string & id) const;

    // Forgets states that can no longer take effect.
    void prune(int64_t sharedNow);

    // Every state still stored: what a SNAPSHOT carries.
    std::vector<TimelineState> all() const;
    std::vector<std::string> ids() const;

    void clear() { timelines.clear(); }

private:
    std::map<std::string, std::vector<TimelineState>> timelines; // each sorted by version
};

} // namespace ofxSyncing
