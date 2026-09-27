#pragma once

// ofxSyncing: events and timelines kept in step across Raspberry Pis.
//
// The core depends only on the openFrameworks core and the standard library.
// Transports are separate headers, included when an app wants them:
//
//   #include "ofxSyncingOsc.h"        // needs ofxOsc in addons.make
//   #include "ofxSyncingWebSocket.h"  // needs ofxHTTP (and its dependencies)
//   #include "ofxSyncingAvahi.h"      // Bonjour discovery; needs libavahi-client-dev

#include "ofxSyncingClock.h"
#include "ofxSyncingCodec.h"
#include "ofxSyncingConfig.h"
#include "ofxSyncingDiscovery.h"
#include "ofxSyncingEstimator.h"
#include "ofxSyncingLoopback.h"
#include "ofxSyncingNet.h"
#include "ofxSyncingNode.h"
#include "ofxSyncingTimeline.h"
#include "ofxSyncingTransport.h"
#include "ofxSyncingTypes.h"
