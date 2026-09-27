#pragma once

#include <string>

#include "ofJson.h"

#include "ofxSyncingTypes.h"

// Message <-> JSON, for the WebSocket adapter, the browser client and the
// multicast beacons. ofJson is part of the openFrameworks core.
//
//   {"type":"ping","group":"default","node":"pi2","term":3,"seq":7,"t0":81234567890}
//
// Only the fields that mean something for the type are written. Every time
// is an integer count of µs, which a browser holds exactly (to 2^53 µs, or
// 285 years).

namespace ofxSyncing {

ofJson toJson(const Message & msg);

// False if it isn't a sync message.
bool fromJson(const ofJson & json, Message & msg);

std::string encodeJson(const Message & msg);
bool decodeJson(const std::string & text, Message & msg);

} // namespace ofxSyncing
