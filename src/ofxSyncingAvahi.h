#pragma once

// Bonjour discovery through Avahi: every node advertises an _ofxsyncing._udp
// service and browses for the others.
//
// Optional and header-only, because it needs libavahi-client-dev, which Pi OS
// doesn't install by default:
//
//   sudo apt install libavahi-client-dev
//
// and in the app's config.make:
//
//   PROJECT_LDFLAGS = -lavahi-client -lavahi-common
//
// Then pass it to Node::setup():
//
//   sync.setup(config, osc, std::make_unique<ofxSyncing::AvahiDiscovery>());
//
// mDNS stays on the local network, like the multicast beacons. What it adds
// is that the nodes show up in any Bonjour browser (avahi-browse -r
// _ofxsyncing._udp), and that avahi-daemon does the announcing.

#include <map>
#include <mutex>
#include <string>

#include <net/if.h>

#include <avahi-client/client.h>
#include <avahi-client/lookup.h>
#include <avahi-client/publish.h>
#include <avahi-common/address.h>
#include <avahi-common/alternative.h>
#include <avahi-common/error.h>
#include <avahi-common/malloc.h>
#include <avahi-common/strlst.h>
#include <avahi-common/thread-watch.h>

#include "ofLog.h"

#include "ofxSyncing.h"

namespace ofxSyncing {

class AvahiDiscovery: public Discovery {
public:
    static constexpr const char * SERVICE_TYPE = "_ofxsyncing._udp";

    ~AvahiDiscovery() {
        stop();
    }

    std::string getName() const override { return "bonjour"; }

    bool start(Node & node) override {
        stop();

        const Config & config = node.getConfig();
        group = config.group;
        nodeId = node.getNodeId();
        serviceName = nodeId;
        port = (uint16_t)config.port;
        ifindex = AVAHI_IF_UNSPEC;
        if (!config.interface.empty()) {
            unsigned index = if_nametoindex(config.interface.c_str());
            if (index) ifindex = (AvahiIfIndex)index;
            else ofLogWarning("ofxSyncing") << "no interface " << config.interface << " for Bonjour; using all";
        }

        poll = avahi_threaded_poll_new();
        if (!poll) {
            ofLogError("ofxSyncing") << "Avahi: couldn't create a poll";
            return false;
        }

        // Waits for avahi-daemon if it isn't running yet.
        int error = 0;
        client = avahi_client_new(avahi_threaded_poll_get(poll), AVAHI_CLIENT_NO_FAIL, &AvahiDiscovery::onClient, this, &error);
        if (!client) {
            ofLogError("ofxSyncing") << "Avahi: " << avahi_strerror(error);
            avahi_threaded_poll_free(poll);
            poll = nullptr;
            return false;
        }
        avahi_threaded_poll_start(poll);
        return true;
    }

    void stop() override {
        if (poll) avahi_threaded_poll_stop(poll);
        // Freeing the client frees the entry group, the browser and any
        // resolvers still attached to it.
        if (client) avahi_client_free(client);
        if (poll) avahi_threaded_poll_free(poll);
        client = nullptr;
        entryGroup = nullptr;
        browser = nullptr;
        poll = nullptr;

        std::lock_guard<std::mutex> lock(mutex);
        found.clear();
    }

    std::vector<Endpoint> getEndpoints() const override {
        std::lock_guard<std::mutex> lock(mutex);
        std::vector<Endpoint> result;
        for (auto & f : found) result.push_back(f.second);
        return result;
    }

private:
    // All the callbacks run on Avahi's poll thread.

    static void onClient(AvahiClient * c, AvahiClientState state, void * data) {
        auto self = (AvahiDiscovery *)data;
        switch (state) {
            case AVAHI_CLIENT_S_RUNNING:
                self->publish(c);
                self->browse(c);
                break;
            case AVAHI_CLIENT_S_COLLISION:
            case AVAHI_CLIENT_S_REGISTERING:
                if (self->entryGroup) avahi_entry_group_reset(self->entryGroup);
                break;
            case AVAHI_CLIENT_CONNECTING:
                ofLogNotice("ofxSyncing") << "waiting for avahi-daemon";
                break;
            case AVAHI_CLIENT_FAILURE:
                ofLogError("ofxSyncing") << "Avahi: " << avahi_strerror(avahi_client_errno(c));
                break;
        }
    }

    void publish(AvahiClient * c) {
        if (!entryGroup) entryGroup = avahi_entry_group_new(c, &AvahiDiscovery::onEntryGroup, this);
        if (!entryGroup || !avahi_entry_group_is_empty(entryGroup)) return;

        std::string groupTxt = "group=" + group;
        std::string nodeTxt = "node=" + nodeId;
        while (true) {
            int result = avahi_entry_group_add_service(entryGroup, ifindex, AVAHI_PROTO_INET, (AvahiPublishFlags)0,
                                                       serviceName.c_str(), SERVICE_TYPE, nullptr, nullptr, port,
                                                       groupTxt.c_str(), nodeTxt.c_str(), (const char *)nullptr);
            if (result == AVAHI_ERR_COLLISION) {
                rename();
                continue;
            }
            if (result < 0) {
                ofLogError("ofxSyncing") << "Avahi: couldn't advertise " << serviceName << ": " << avahi_strerror(result);
                return;
            }
            break;
        }
        avahi_entry_group_commit(entryGroup);
    }

    // Another service already has this name: take the next one Avahi offers.
    void rename() {
        char * next = avahi_alternative_service_name(serviceName.c_str());
        serviceName = next;
        avahi_free(next);
    }

    static void onEntryGroup(AvahiEntryGroup * g, AvahiEntryGroupState state, void * data) {
        auto self = (AvahiDiscovery *)data;
        if (state == AVAHI_ENTRY_GROUP_COLLISION) {
            self->rename();
            avahi_entry_group_reset(g);
            self->publish(avahi_entry_group_get_client(g));
        } else if (state == AVAHI_ENTRY_GROUP_FAILURE) {
            ofLogError("ofxSyncing") << "Avahi: " << avahi_strerror(avahi_client_errno(avahi_entry_group_get_client(g)));
        }
    }

    void browse(AvahiClient * c) {
        if (browser) return;
        browser = avahi_service_browser_new(c, ifindex, AVAHI_PROTO_INET, SERVICE_TYPE, nullptr, (AvahiLookupFlags)0,
                                            &AvahiDiscovery::onBrowse, this);
        if (!browser) {
            ofLogError("ofxSyncing") << "Avahi: couldn't browse: " << avahi_strerror(avahi_client_errno(c));
        }
    }

    static std::string keyOf(AvahiIfIndex interface, const char * name) {
        return std::to_string(interface) + "/" + name;
    }

    static void onBrowse(AvahiServiceBrowser * b, AvahiIfIndex interface, AvahiProtocol protocol, AvahiBrowserEvent event,
                         const char * name, const char * type, const char * domain, AvahiLookupResultFlags, void * data) {
        auto self = (AvahiDiscovery *)data;
        if (event == AVAHI_BROWSER_NEW) {
            // The resolver frees itself in onResolve.
            avahi_service_resolver_new(avahi_service_browser_get_client(b), interface, protocol, name, type, domain,
                                       AVAHI_PROTO_INET, (AvahiLookupFlags)0, &AvahiDiscovery::onResolve, self);
        } else if (event == AVAHI_BROWSER_REMOVE) {
            std::lock_guard<std::mutex> lock(self->mutex);
            self->found.erase(keyOf(interface, name));
        }
    }

    static void onResolve(AvahiServiceResolver * r, AvahiIfIndex interface, AvahiProtocol, AvahiResolverEvent event,
                          const char * name, const char *, const char *, const char *, const AvahiAddress * address,
                          uint16_t port, AvahiStringList * txt, AvahiLookupResultFlags, void * data) {
        auto self = (AvahiDiscovery *)data;
        if (event == AVAHI_RESOLVER_FOUND && address) {
            std::string theirGroup = txtValue(txt, "group");
            std::string theirNode = txtValue(txt, "node");
            if (theirGroup == self->group && theirNode != self->nodeId) {
                char ip[AVAHI_ADDRESS_STR_MAX];
                avahi_address_snprint(ip, sizeof(ip), address);
                std::lock_guard<std::mutex> lock(self->mutex);
                self->found[keyOf(interface, name)] = Endpoint(ip, port);
            }
        }
        avahi_service_resolver_free(r);
    }

    static std::string txtValue(AvahiStringList * txt, const char * key) {
        AvahiStringList * item = avahi_string_list_find(txt, key);
        if (!item) return "";
        char * k = nullptr;
        char * v = nullptr;
        std::string result;
        if (avahi_string_list_get_pair(item, &k, &v, nullptr) == 0 && v) result = v;
        avahi_free(k);
        avahi_free(v);
        return result;
    }

    std::string group;
    std::string nodeId;
    std::string serviceName;
    uint16_t port = 0;
    AvahiIfIndex ifindex = AVAHI_IF_UNSPEC;

    AvahiThreadedPoll * poll = nullptr;
    AvahiClient * client = nullptr;
    AvahiEntryGroup * entryGroup = nullptr;
    AvahiServiceBrowser * browser = nullptr;

    mutable std::mutex mutex;
    std::map<std::string, Endpoint> found;  // by interface and service name
};

} // namespace ofxSyncing
