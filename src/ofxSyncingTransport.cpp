#include "ofxSyncingTransport.h"

#include "ofxSyncingClock.h"
#include "ofxSyncingNode.h"

namespace ofxSyncing {

int64_t Transport::stampReceive() const {
    Node * n = node;
    return n ? n->getLocalTimeUs() : monotonicUs();
}

void Transport::deliver(const Message & msg, const Endpoint & from, int64_t rxUs) const {
    if (Node * n = node) n->receive(msg, from, rxUs);
}

void PeerListTransport::broadcast(const Message & msg) {
    for (auto & peer : getPeers()) {
        send(peer, msg);
    }
}

void PeerListTransport::setPeers(const std::vector<Endpoint> & p) {
    std::lock_guard<std::mutex> lock(peersMutex);
    peers = p;
}

std::vector<Endpoint> PeerListTransport::getPeers() const {
    std::lock_guard<std::mutex> lock(peersMutex);
    return peers;
}

} // namespace ofxSyncing
