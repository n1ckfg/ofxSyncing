meta:
	ADDON_NAME = ofxSyncing
	ADDON_DESCRIPTION = Synchronize events and timelines across Raspberry Pis over OSC, WebSockets or any other transport.
	ADDON_AUTHOR = Nick Fox-Gieg
	ADDON_TAGS = "sync" "networking" "clock" "osc" "websocket" "raspberry pi"
	ADDON_URL = https://github.com/n1ckfg/ofxSyncing

common:
	# The core needs only the openFrameworks core. Transport adapters are
	# header-only, so an app that includes one lists its addon (ofxOsc,
	# ofxHTTP) in its own addons.make.
