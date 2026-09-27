################################################################################
# example_osc: sync over OSC (UDP) on a local network.
# Build with `make`, run with `make RunRelease` or `./bin/example_osc`.
################################################################################

OF_ROOT = ../../..

# Bonjour discovery (<discovery>bonjour</discovery> in settings.xml) needs
# Avahi's headers: sudo apt install libavahi-client-dev, then uncomment these.
# PROJECT_DEFINES = OFXSYNCING_AVAHI
# PROJECT_LDFLAGS = -lavahi-client -lavahi-common
