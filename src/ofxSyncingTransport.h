#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

#include "ofxSyncingTypes.h"

namespace ofxSyncing {

class Node;

// Moves Messages between nodes. The core never pulls in a network library;
// adapters do, and each is a single header an app includes when it wants it
// (ofxSyncingOsc.h, ofxSyncingWebSocket.h).
//
// What an adapter must do:
// - Stamp every received message with stampReceive() on its own receive
//   thread, as close to the socket as it can, then hand it to deliver().
//   Stamping when update() dequeues it would add up to a frame (16.7 ms) of
//   error to every round-trip measurement.
// - Send synchronously. The node stamps PINGs and PONGs right before calling
//   send(), so a message queued for a later frame carries a wrong time.
// - Accept send() and broadcast() from any thread.
// - Give deliver() a `from` that send() can reach the sender at.
class Transport {
public:
    virtual ~Transport() = default;

    virtual bool send(const Endpoint & to, const Message & msg) = 0;

    // Sends to every peer the transport can reach.
    virtual void broadcast(const Message & msg) = 0;

    // The endpoints broadcast() should reach, for transports that can't find
    // them on their own. The node calls this when discovery changes.
    virtual void setPeers(const std::vector<Endpoint> & peers) {}

    // Whether an endpoint is this node's own, so broadcasts leave it out.
    virtual bool isLocal(const Endpoint & endpoint) const { return false; }

    // Node::setup() attaches itself.
    void attach(Node * n) { node = n; }
    Node * getNode() const { return node; }

protected:
    // The node's local clock, now. Call it first thing on the receive thread.
    int64_t stampReceive() const;

    // Hands a received message to the node.
    void deliver(const Message & msg, const Endpoint & from, int64_t rxUs) const;

    std::atomic<Node *> node{nullptr};
};

// A connectionless transport: broadcast() sends to each peer in turn.
class PeerListTransport: public Transport {
public:
    void broadcast(const Message & msg) override;
    void setPeers(const std::vector<Endpoint> & peers) override;
    std::vector<Endpoint> getPeers() const;

private:
    mutable std::mutex peersMutex;
    std::vector<Endpoint> peers;
};

} // namespace ofxSyncing
