# ofxSyncing

An openFrameworks addon that keeps events and timelines in step across Raspberry Pis on a network.

Every node keeps a clock shared with the rest of its group. Events are scheduled for a moment on that clock, a little in the future, so every node fires them at the same time, however long the message took to arrive. Timelines are state, not a stream of events, and every node computes the same position from the shared clock.

The core is independent of the network protocol and depends only on the openFrameworks core. It ships with OSC and WebSocket transports, and another protocol only needs a small adapter.

- **Wired Ethernet:** the clocks of any two nodes agree within ±1 ms (p95).
- **WiFi, power saving off:** within ±5 ms.
- **WebSocket over the internet:** best effort. Each node reports its estimated error, and lead times grow to match.
- **Dispatch:** events fire within 1 ms of their time on the scheduler thread.

The addon can't control output latency: a display shows a frame up to one refresh late (and each Pi's refresh is out of phase with the others), and audio has its buffers. Set `output_latency_us` per node and events fire that much early.

## Contents

| | |
|---|---|
| `src/` | The core, plus the transport adapters and Bonjour discovery as single headers |
| `example_osc/` | Sync over OSC on a local network: a flash every second, a moving bar, GPIO pulses |
| `example_websocket/` | Sync over WebSockets, across the internet, with a browser client |
| `tests/` | Unit tests (ofxUnitTests), including simulated networks and 8 hours of drift |

## Quick start

`addons.make`:

```
ofxOsc
ofxSyncing
```

`ofApp.h`:

```cpp
#include "ofxSyncing.h"
#include "ofxSyncingOsc.h"

class ofApp: public ofBaseApp {
    // ...
    void onSyncEvent(const ofxSyncing::FiredEvent & e);

    // The node first, so the transport is destroyed first and stops
    // delivering before the node goes.
    ofxSyncing::Node sync;
    ofxSyncing::OscTransport osc;
};
```

`ofApp.cpp`:

```cpp
void ofApp::setup() {
    ofxSyncing::Config config;
    config.load(ofToDataPath("settings.xml"));  // or set the fields in code

    osc.setup(config);
    sync.setup(config, osc);
    ofAddListener(sync.onEvent, this, &ofApp::onSyncEvent);
    sync.start();
}

void ofApp::update() {
    sync.update();  // delivers events to onEvent, on this thread
}

void ofApp::keyPressed(int key) {
    // Fires on every node at the same moment, about 50 ms from now.
    sync.scheduleEvent("flash", "red");

    if (key == 'p') sync.play("show");
}

void ofApp::onSyncEvent(const ofxSyncing::FiredEvent & e) {
    // e.event.name, e.event.payload, e.event.time (shared µs),
    // and e.latenessUs: how long after its time it got here.
}

void ofApp::draw() {
    // The same position on every node, computed from the shared clock.
    double seconds = sync.getTimelinePosition("show");
}
```

Every node runs the same app. The one with the highest `priority` becomes master; the rest follow it.

## How it works

### The shared clock

The shared clock is built on `CLOCK_MONOTONIC`, the source for `ofGetElapsedTimeMicros()`, and never on the system clock. `systemd-timesyncd` steps and slews the system clock, and a Pi has no RTC. The addon never sets the OS clock.

Each node keeps an offset and a drift rate on top of its monotonic clock:

```
shared = local + offset + skew * (local - ref)
```

The master's shared clock is the reference. Followers measure it with NTP-style exchanges of four timestamps (`offset = ((t1 - t0) + (t2 - t3)) / 2`, `delay = (t3 - t0) - (t2 - t1)`):

- **Initial lock:** a burst of 8 pings about 50 ms apart. It keeps the lowest-delay quarter and takes the median offset.
- **Ongoing:** 1 ping per second (`ping_hz`) into a sliding window of 32 samples. WiFi and TCP delays spike, and a spike makes a sample's offset wrong by up to half of it, so each estimate uses only the minimum-delay samples. As in NTP's clock filter, a sample's error bound is half its delay plus its age times the drift uncertainty, so while the drift is unknown a fresh sample beats an old one.
- **Drift:** a linear fit of offset over several minutes of history. A Pi's crystal drifts tens of ppm, roughly 50–150 ms an hour if uncorrected. On a quiet network, a provisional fit over the window takes over within about 5 s.
- **Corrections** are slewed at up to 500 µs per second (`slew_rate_us`), so shared time never runs backwards. An error above `step_threshold_ms` (default 20 ms) triggers a fresh burst, and the clock steps if the burst confirms it. That fires `onDiscontinuity`.

Every timestamp is an `int64_t` count of microseconds. (OSC's float32 would lose precision within hours.)

`isLocked()` says whether shared time means anything yet. A follower locks about half a second after it finds the master.

### Master selection

- The master is the live node with the highest `priority`, with ties broken by `node_id` (the hostname by default). A node with priority 0 is never master.
- A starting node listens for two beacon intervals before joining an election. A node that finds a master already running follows it, whatever its priority. There is no preemption, which prevents flapping.
- Followers declare the master lost after it has been silent for `master_timeout_ms` (default 3 s). The next candidate takes over with `term + 1`, **continuing from its own shared time**, so timelines don't jump, and it takes over the pending events.
- Messages from a master with a stale `term` are ignored.
- When a network split heals and two masters see each other, the higher term wins, then the higher priority. The losing side re-locks to the winner and fires `onDiscontinuity` with the reason `MasterConflict`, plus a `ClockStep` if its clock had to step. The winner's timelines replace the loser's.

`onRoleChange` reports every change of role, master or term.

### Events

```cpp
EventId scheduleEvent(name, payload = "", leadUs = 0, LatePolicy = FireLate);
EventId scheduleEventAt(name, sharedTimeUs, payload = "", LatePolicy = FireLate);
```

- On the master, an event is given a time `T` and sent to every node, and each fires it locally at `T`.
- On a follower, `scheduleEvent` sends an `EVENT_REQUEST`, and the master assigns `T`. That keeps a single authority on ordering. Requests are resent until the master confirms them.
- **Lead time:** `max(lead_floor_ms, lead_k × p99 round trip across followers)`. On a LAN that's the 50 ms floor; over the internet it grows on its own. `getLeadTimeUs()` reports it. `leadUs` can ask for longer; `scheduleEventAt` sets the time itself.
- **Delivery:** followers acknowledge every event, and the master resends to any that haven't until `T`. Nodes de-duplicate by event id, so neither a resend nor a clock step fires an event twice.
- **Late events:** `FireLate` (the default) fires immediately and reports how late; `DropLate` drops the event if it's later than `late_tolerance_ms`.
- Payloads are opaque strings. Keep them text if a WebSocket client will see them (JSON carries text).

### Dispatch

A scheduler thread sleeps with `clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME)` until the next event is due. Every event reaches you twice, with its `latenessUs`:

- **`onEventThread`**, on the scheduler thread, the moment it's due. This has the tightest timing, for GPIO or audio. Keep listeners short and thread-safe: a slow one delays the events after it.
- **`onEvent`**, from `update()` on the main thread. It can be up to a frame late; `latenessUs` lets animations correct for that. For visuals, drive what's on screen from the event's `time` and the shared clock (as the examples do) rather than from when the event arrived.

With `output_latency_us` set, events fire that much before their time, and `getOutputTimeUs()` and `getTimelinePosition()` look that far ahead.

For tighter dispatch, set `realtime_priority` (1–99) to run the scheduler thread `SCHED_FIFO` (see the Pi notes).

### Timelines

A timeline is state, `{timeline_id, version, T0, P0, rate}`, and every node computes

```
position(now) = P0 + (shared_now - T0) × rate
```

```cpp
sync.play("show");
sync.pause("show");
sync.seek("show", 30 * 1000000);  // µs
sync.setRate("show", 0.5);
sync.changeTimeline("show", setPosition, positionUs, setRate, rate, leadUs, atSharedUs);

double seconds = sync.getTimelinePosition("show");  // at output time
TimelineState state = sync.getTimeline("show");      // .isPlaying(), .rate
```

- Changes take effect at a future shared time, like events, and a follower's changes go through the master.
- A timeline exists once something changes it. Before its first change takes effect, it sits at position 0.
- For visuals, compute positions from the shared clock in `draw()` rather than waiting for events.
- The master repeats every timeline once a beacon interval, so a lost change heals within a second.
- A node that joins late gets a `SNAPSHOT` of every timeline and pending event from the master.

### Status

```cpp
Status st = sync.getStatus();  // role, term, master, shared time, lead, events fired and dropped,
                               // and st.stats: locked, errorUs, delayUs, delayP99Us, driftPpm
for (auto & p : sync.getPeers()) { /* p.nodeId, p.live, p.isMaster, p.stats as the peer reports them */ }
```

Each node reports its estimated error (bounded by half the minimum delay, plus whatever is still being slewed in), its round trip to the master and its drift, in its beacons. The master uses the round trips for the lead time, and every node can show the whole group.

## Configuration

The addon takes an `ofxSyncing::Config`. `Config::load()` fills it from a flat `settings.xml`, and `Config::set(name, value)` sets one setting by the same name, so command-line flags can override the file (the examples take `--priority 200` and so on).

| Setting | Default | |
|---|---|---|
| `group` | `default` | Nodes only sync within a group, so two installations can share a network |
| `node_id` | hostname | |
| `priority` | `100` | Highest live priority becomes master; 0 never does |
| `interface` | any | Announce and accept peers on this interface only, e.g. `eth0` |
| `discovery` | `beacon` | `beacon`, `static` or `bonjour` |
| `peers` | | Static peers, `<peer>host</peer>` or `<peer>host:port</peer>` |
| `port` | `9400` | The transport's port |
| `multicast_group`, `multicast_port` | `239.255.94.1`, `9401` | For beacon discovery |
| `ping_hz` | `1` | Clock pings per second after the initial burst |
| `lead_floor_ms` | `50` | The least lead time an event gets |
| `lead_k` | `3` | Lead time is at least `lead_k` × the slowest p99 round trip |
| `late_tolerance_ms` | `10` | How late a `drop_late` event may still fire |
| `step_threshold_ms` | `20` | Errors past this step the clock; smaller ones are slewed |
| `slew_rate_us` | `500` | µs of correction per second |
| `output_latency_us` | `0` | Fire this much early to cover display or audio latency |
| `master_timeout_ms` | `3000` | A master silent this long is lost |
| `beacon_interval_ms` | `1000` | |
| `auth_token` | | WebSocket only: clients must present it to schedule events or change timelines |
| `csv_log` | | Log offset, delay, error and drift for this node and every peer (see Verification) |
| `realtime_priority` | `0` | 1–99 runs the scheduler thread `SCHED_FIFO` |

`example_osc/bin/data/settings.xml` has a commented copy.

## Transports

The core deals in `ofxSyncing::Message`. An adapter serializes it for its protocol, implements `send(peer, msg)` and `broadcast(msg)`, and hands what arrives to the node. Adapters are single headers, so the core never pulls in `ofxOsc` or `ofxHTTP`; an app that includes one adds its addon to its own `addons.make`.

### OSC (`ofxSyncingOsc.h`)

For a local network, over UDP. It needs only `ofxOsc`, which ships with openFrameworks.

- Each message type has its own address, `/ofxSyncing/<type>`, and its fields map to typed OSC arguments, `int64` for every time. The first four arguments are always group, node id, term and the sender's listening port: UDP replies come from a different port than the one a node listens on.
- Receive timestamps come from a subclass of `ofxOscReceiver` that overrides the protected `ProcessMessage()`, which runs on the receiver's listener thread.
- With `interface` set, it listens on that interface's address only.
- A message must fit in one UDP datagram (about 64 KB), which limits how many pending events and timelines a snapshot can carry.

### WebSocket (`ofxSyncingWebSocket.h`)

For sync beyond the local network, over TCP. It's a star: the master runs an `ofxHTTP::SimpleWebSocketServer` (`WebSocketServerTransport`), and each follower connects with a `Poco::Net::WebSocket` on a thread of its own (`WebSocketClientTransport`), reconnecting with backoff. Followers can only reach the master, so they get priority 0 and never take over.

- Frames are JSON (`ofxSyncingCodec.h`), one message per text frame, so a browser can take part.
- The server raises ofxHTTP's frame buffer (Poco drops a frame that doesn't fit) and its poll timeout drops to 1 ms. ofxHTTP's connection loop blocks for that timeout twice per turn, and a PING that arrives during the wait sits there before it's read, which skews the offset. At ofxHTTP's default of 10 ms, round trips on one machine were 4–6 ms; at 1 ms they're about 0.6 ms.
- The server answers a PING from inside that connection's frame callback, so the PONG goes out the moment the callback returns. Other messages meant for one client are broadcast with that client's node id in `"to"`, because a connection object can't safely be used from another thread; the other clients ignore them.
- Clients set `TCP_NODELAY`, so Nagle doesn't hold PINGs back. ofxHTTP gives no way to set it on the server's side, so a PONG can occasionally wait for an earlier frame's acknowledgement; its extra delay gets the sample discarded by the minimum-delay filter.
- **Auth:** a client presents `auth_token` (`{"type":"auth","token":"..."}`) before the server accepts its event requests or timeline changes. Without it, a client can still sync and watch. The server also refuses anything from a client that claims to be master.
- It needs `ofxHTTP` and its dependencies (`ofxIO ofxMediaType ofxNetworkUtils ofxPoco ofxSSLManager`). On 64-bit Pi OS, `ofxPoco`'s `addon_config.mk` has no `linuxaarch64` section; `example_websocket/setup.sh` adds it.

### Writing an adapter

Subclass `ofxSyncing::Transport` (or `PeerListTransport`, whose `broadcast()` sends to each peer in turn), and:

- **Stamp every received message on your receive thread**, as close to the socket as you can, with `stampReceive()`, and pass it to `deliver(msg, from, rxUs)`. Stamping when `update()` dequeues it adds up to a frame (16.7 ms) of error to every round-trip measurement.
- **Send synchronously.** The node stamps PINGs and PONGs right before it calls `send()`, so a message queued for a later frame carries the wrong time.
- Accept `send()` and `broadcast()` from any thread.
- Make `from` an endpoint `send()` can reach the sender at.

`ofxSyncingCodec.h` converts messages to and from JSON if your protocol carries text.

## Discovery

Discovery is pluggable, because its scope depends on the network: multicast and mDNS don't cross the internet.

- **Beacon** (the default, no dependencies): every node sends its `BEACON` to a multicast group (`multicast_group`, `multicast_port`) once a second and learns its peers from theirs. Nodes on the same machine each get a copy.
- **Static:** only the hosts in `peers`. Names are resolved with plain `getaddrinfo` on a thread of their own and re-resolved periodically. `.local` names already resolve through `libnss-mdns`, so no Bonjour library is needed. Use this where multicast doesn't get through, e.g. on some WiFi networks.
- **Bonjour** (`ofxSyncingAvahi.h`, optional): advertises and browses an `_ofxsyncing._udp` service through avahi-daemon. It needs `libavahi-client-dev`, which isn't installed by default:

  ```
  sudo apt install libavahi-client-dev
  ```

  In the app's `config.make`, add `PROJECT_LDFLAGS = -lavahi-client -lavahi-common`, and pass the backend in:

  ```cpp
  #include "ofxSyncingAvahi.h"
  sync.setup(config, osc, std::make_unique<ofxSyncing::AvahiDiscovery>());
  ```

  `example_osc/config.make` has these lines commented out.

With `interface` set (e.g. `eth0`), a node announces that interface's address and sends, joins and listens for beacons on that interface only. A Pi with `eth0` and `wlan0` on the same subnet could otherwise sync over WiFi without anyone noticing, and the node warns at startup when it sees that setup.

Beacons also carry each node's role, term, priority and sync stats. Liveness comes from the beacons that travel over the transport itself, since multicast on WiFi is unreliable.

## Examples

### example_osc

```
cd example_osc
make
make RunRelease          # or ./bin/example_osc
```

Run it on every Pi. The screen shows the node's role, term and shared clock, its error, round trip and drift, and the same for each peer. There's a flash on every whole second of shared time, a moving bar driven by a timeline, and recent role changes and discontinuities.

| Key | |
|---|---|
| space, `e` | Schedule a flash in a random color (from any node) |
| `p` | Play or pause the timeline |
| ← → | Seek back or forward a second |
| `0` | Rewind |
| `r` | Change rate: 1, 2, 0.5 |
| `k` | Kill this node (it goes silent) or bring it back. Kill the master to test failover |
| `f` | Fullscreen |

**Without a display** (over ssh, or a Pi with no desktop), or with `--headless`, it prints its status every second and reads the same keys from stdin (type the key, then return; `<` and `>` seek).

**Two nodes on one Pi**, to try it without a second one:

```
./bin/example_osc --node_id pi-a --priority 200 --port 9400
./bin/example_osc --node_id pi-b --priority 100 --port 9402
```

Any setting in `settings.xml` works as a flag; `--settings other.xml` reads another file.

**GPIO:** set `gpio_line` to a BCM pin number and every event pulses it for `gpio_pulse_us`, from the scheduler thread. It uses the Linux GPIO character device directly (no library); on a Pi 4 the header is `/dev/gpiochip0`. The `pi` user is in the `gpio` group already.

### example_websocket

```
cd example_websocket
./setup.sh               # once: libpoco-dev, ofxHTTP and its addons, the ofxPoco patch
make
```

One app, two roles. With `ws_connect` empty it's the master: it runs the WebSocket server and serves the browser client. With `ws_connect` set to the master's address, it's a follower:

```
./bin/example_websocket                                           # the master
./bin/example_websocket --ws_connect pi-1 --node_id pi-2          # a follower
```

Open `http://<master>:9400/` in a browser for the browser client, which shows the shared clock, the flashes and the bar, and with the auth token can schedule flashes and play the timeline. Browser timers are coarse, so it's a demo of the protocol, not a precision client.

Sync over the internet is looser than on a LAN. The screen shows the estimated error and the lead time, which grows with the slowest round trip.

**External access:**

- **Tailscale (recommended).** Install it on every node (`curl -fsSL https://tailscale.com/install.sh | sh`, then `sudo tailscale up`) and connect to the master's Tailscale name or `100.x` address. It needs no port forwarding, it's encrypted, and access is controlled by your tailnet. The master lists its addresses, Tailscale's included, when it starts.
- **Port forwarding.** Forward the port to the master and use `wss://`, since browsers on https pages refuse plain `ws://`. Set `ws_ssl` to 1 on both sides and put a certificate for the master's public name in its `bin/data/ssl/` as `certificate.pem` and `privateKey.pem` (Let's Encrypt works). Followers check it against the system's CAs.
- **Either way,** set `auth_token` to something long and random: clients must present it before they can schedule events or change timelines.

If the master restarts, followers reconnect within 2 s and it takes over again. A master that restarts and hears no follower before it takes over starts a new timebase, and the followers step to it once (`onDiscontinuity`).

## Verification

### Unit tests

```
cd tests
make
./bin/tests              # the exit code is the number of failures
```

They cover the offset math, sample filtering, the drift fit, slewing and stepping, de-duplication, late policies, output latency, timeline positions and versions, both wire formats, master selection, failover, a returning node, a network split and heal, and a node joining late. Most run on `ofxSyncing::Simulation` (`ofxSyncingLoopback.h`): an in-process network on simulated time, with delay, jitter, loss and one-way spikes, and a simulated crystal per node. That runs 8 hours of drift in about 3 seconds, and a seed makes every run the same. The last test runs two nodes on real UDP sockets and threads.

Results on a Pi 4:

| Scenario | Spread between clocks (p95) |
|---|---|
| Wired LAN, simulated (150 ± 50 µs, crystals ±110 ppm) | 0.03 ms |
| WiFi, simulated (2 ± 1 ms, 1% loss, 20% of packets spiking up to 40 ms) | 0.46 ms |
| `netem delay 20ms 10ms loss 5%`, simulated | 3.7 ms |
| 8 hours, crystals 200 ppm apart, simulated | 0.03 ms (max 0.05 ms) |
| Two processes on one Pi, real UDP | 0.07–0.12 ms |

On one Pi, events fired 0.06–0.1 ms after their time on the scheduler thread, and within 0.1 ms of each other across the two processes. Two processes on one machine share its cores, so their receive threads wake unevenly, and that asymmetry (about 0.06 ms) is most of what's left. Two Pis each wake on their own network interrupt.

These numbers come from simulations and from one Pi. The precision between separate Pis has to be measured on real hardware, as below.

### Hardware measurement

- **GPIO:** set `gpio_line` on every Pi running `example_osc`, and every tick pulses the pin at the same scheduled time. Measure the skew between pins with a logic analyzer or scope. That measures clock sync plus dispatch.
- **Display:** a photodiode on each screen (or a high-speed camera filming them all) measures the full path, display included. Use the difference to set `output_latency_us` per node.

### Network impairment

On a wired network, add delay, jitter and loss with netem, and remove it afterwards:

```
sudo tc qdisc add dev eth0 root netem delay 20ms 10ms loss 5%
sudo tc qdisc del dev eth0 root
```

### Scenarios

With `example_osc` on three or more Pis: kill the master (`k`) and watch the next take over with `term + 1`, with the bar carrying on; bring it back and watch it follow; unplug a Pi and plug it back in (a split and heal); start a Pi late; leave it running overnight.

### CSV logging

Set `csv_log` (e.g. `sync.csv`, in `bin/data`) and each node appends a row for every clock sample of its own (`self`) and every beacon from a peer (`peer`):

```
local_us,shared_us,kind,node,master,term,offset_us,delay_us,error_us,drift_ppm,locked
```

The master's log therefore has the whole group's error, round trip and drift over time.

## Raspberry Pi setup notes

- **Prefer wired Ethernet, and set `interface` to it** (`<interface>eth0</interface>`).
- **Turn off WiFi power saving.** It adds tens to hundreds of ms of jitter:

  ```
  sudo iw wlan0 set power_save off
  ```

  To make it persistent with NetworkManager (Pi OS Bookworm), find the connection with `nmcli connection show`, then:

  ```
  sudo nmcli connection modify <connection> 802-11-wireless.powersave 2
  ```

- **eth0 and wlan0 on the same subnet:** a Pi with both up answers ARP for either address on either interface ("ARP flux"), so traffic meant for its Ethernet address can arrive over WiFi. Set `interface`, and either put WiFi on another subnet, turn it off (`sudo nmcli radio wifi off`), or have each interface answer only for its own address:

  ```
  echo -e 'net.ipv4.conf.all.arp_ignore = 1\nnet.ipv4.conf.all.arp_announce = 2' | sudo tee /etc/sysctl.d/90-arp.conf
  sudo sysctl --system
  ```

- **`systemd-timesyncd` can stay on.** The addon doesn't use the system clock.
- **Realtime scheduling (optional):** to let `realtime_priority` run the scheduler thread `SCHED_FIFO`, allow the user an rtprio limit and log in again:

  ```
  echo 'pi - rtprio 50' | sudo tee /etc/security/limits.d/ofxsyncing.conf
  ```

  Without it, the node logs a warning and carries on at normal priority.
- **Multicast:** beacon discovery needs multicast between the Pis. Most switches pass it; some WiFi access points drop or delay it. If peers don't find each other, use `static` discovery.
- **Firewall:** if one is enabled, open the transport port (UDP for OSC, TCP for WebSockets) and, for beacons, UDP 9401.

## Protocol

Every message carries a `group`, so installations on the same network ignore each other, plus the sender's `node_id` and the master's `term`.

| Type | Fields | Purpose |
|---|---|---|
| `BEACON` | priority, address, port, is_master, master, shared time, lead, sync stats | Discovery, liveness, and stats for the whole group |
| `PING` | seq, t0, want_snapshot | Clock sync request (follower to master) |
| `PONG` | seq, t0, t1, t2 | Clock sync reply |
| `EVENT` | event id (origin + seq), name, T, payload, late policy | Scheduled event |
| `ACK` | event id | A follower confirms it received an event |
| `EVENT_REQUEST` | the event, with a requested time or lead | A follower asks the master to schedule an event |
| `TIMELINE` | timeline id, version, T0, P0, rate; version 0 is a request | Timeline state change |
| `SNAPSHOT` | timelines and pending events | Full state for a node that joins late |

In JSON (the WebSocket transport and multicast beacons), a message looks like:

```json
{"type":"ping","group":"default","node":"pi-2","term":3,"seq":7,"t0":81234567890}
```

`ofxSyncingCodec.cpp` has the rest, and `example_websocket/bin/data/DocumentRoot/index.html` is a complete client in JavaScript.
