- This is an openFrameworks addon for use with Raspberry Pis to let them synchronize events and timelines over a network.

- It will be a set of methods independent of transport protocol, allowing it to be used interchangeably with OSC, WebSockets, or other network protocols.

## Precision targets

"The same moment" is measured as the spread between the shared clocks of any two nodes (p95):
- **Wired Ethernet:** ±1 ms.
- **WiFi (power saving off):** ±5 ms.
- **WebSocket over the internet:** best effort. Each node reports its estimated error, and lead times scale up to match.
- **Event dispatch:** events fire within 1 ms of their scheduled time on the scheduler thread (see Dispatch).

The addon can't control output latency: display refresh (up to one frame, and each Pi's refresh is out of phase with the others) and audio buffers. It exposes a per-node `output_latency_us` so that apps can fire events early to compensate for it.

## Architecture

The core depends only on the openFrameworks core and the standard library. It is thread-safe, because transports deliver messages from their own network threads.

### Clock
- Built on `CLOCK_MONOTONIC` (the source for `ofGetElapsedTimeMicros`), never on the system clock. `systemd-timesyncd` steps and slews the system clock, and a Pi has no RTC. The addon never sets the OS clock.
- Shared time: `shared_us = local_monotonic_us + offset_us + skew * (local_monotonic_us - ref_us)`. The addon keeps an estimated offset and drift rate. The underlying clock is never adjusted.
- Every timestamp is an int64 of microseconds. OSC float32 loses precision within hours.
- After the first lock, corrections are **slewed** at a bounded rate (default 500 µs per second), so shared time never runs backwards. If the error goes above a step threshold (default 20 ms), the node re-locks with a step and fires an `onDiscontinuity` event so the app can react.

### Transport interface
- The core deals in a typed `ofxSyncing::Message`. Each transport adapter serializes it: OSC maps fields to typed args, and WebSocket uses JSON.
- An adapter implements `send(peer, msg)` and `broadcast(msg)`, and calls `core.receive(msg, from, rxTimeUs)`.
- **Receive timestamps come from the adapter's receive thread**, as close to the socket as possible. Stamping a message when `update()` dequeues it adds up to a frame (16.7 ms) of error to every round-trip measurement.
- Send timestamps are taken right before the adapter's send call, so adapters must send synchronously, not queue messages for a later frame.
- Adapters are header-only. The core never pulls in `ofxOsc` or `ofxHTTP` unless an app includes an adapter, and that app then lists the dependency in its own `addons.make`.

### Messages
Every message carries a `group` id, so two installations on the same network ignore each other, plus the sender's `node_id` and the master's `term`.

| Type | Fields | Purpose |
|---|---|---|
| `BEACON` | priority, address, port, is_master | Discovery and master liveness |
| `PING` | seq, t0 | Clock sync request (follower to master) |
| `PONG` | seq, t0, t1, t2 | Clock sync reply |
| `EVENT` | event_id (origin + seq), name, T, payload, late_policy | Scheduled event |
| `ACK` | event_id | Follower confirms it received an event |
| `EVENT_REQUEST` | name, payload, lead hint | A non-master node asks the master to schedule an event |
| `TIMELINE` | timeline_id, version, T0, P0, rate | Timeline state change |
| `SNAPSHOT` | timelines + pending events | Full state for a node that joins late |

## Synchronization process

1. **Discovery:** Discovery is its own pluggable module, because its scope depends on the network: mDNS doesn't cross the internet. Backends:
   - **Static list:** hostnames or IPs from `settings.xml`, for predefined network topologies. They are resolved with plain `getaddrinfo`. `.local` names already resolve through `libnss-mdns`, so no Bonjour library is needed, and names are re-resolved periodically.
   - **Multicast beacon** (default, no dependencies): each node sends a `BEACON` to a multicast group once per second.
   - **Bonjour (Avahi):** optional. It advertises and browses an `_ofxsyncing._udp` service and requires `libavahi-client-dev`, which isn't installed by default.
   - A configured `interface` (e.g. `eth0`) sets which address a node announces and accepts peers on. A Pi with `eth0` and `wlan0` on the same subnet could otherwise sync over WiFi without anyone noticing.

2. **Master selection:**
   - The master is the live node with the highest `priority` from `settings.xml`, with ties broken by `node_id` (default: the hostname).
   - Followers declare the master lost after it misses beacons or pongs for a timeout (default 3 s). The next candidate takes over with `term + 1`, **continuing from its current shared time** so timelines don't jump.
   - Messages with a stale `term` are ignored.
   - A returning node joins as a follower; there is no preemption, which prevents flapping.
   - If a network split heals and two masters see each other, the higher term wins, then the higher priority. The loser re-locks and fires `onDiscontinuity`.

3. **Clock synchronization:** Followers exchange NTP-style four-timestamp pings with the master (star topology):
   - `offset = ((t1 - t0) + (t2 - t3)) / 2`, `delay = (t3 - t0) - (t2 - t1)`. This assumes the path is equally long in each direction; WiFi often isn't, which is part of why its target is looser.
   - **Initial lock:** a burst of 8 pings about 50 ms apart. Keep the lowest-delay quarter and take the median offset.
   - **Ongoing:** 1 ping per second (configurable) into a sliding window of 32 samples. Each estimate uses the minimum-delay samples, because WiFi and TCP delays spike and this filter discards the spikes.
   - Each node reports its estimated error (bounded by half the minimum delay), its delay to the master and its drift in ppm.

4. **Event scheduling:** Nodes do not rely on instantaneous message delivery.
   - The master sends "execute event X at future shared time T", and every node triggers it locally at T, so network jitter doesn't affect when it fires.
   - Other nodes request events with `EVENT_REQUEST`, and the master assigns T. That keeps a single authority on ordering.
   - **Lead time:** `max(lead_floor, k × p99 delay across followers)`, where `lead_floor` defaults to 50 ms on a LAN. Over the internet it grows automatically.
   - **Delivery:** followers `ACK` each event id, and the master resends to any follower that hasn't acknowledged until T. Receivers de-duplicate by event id, so a resend or a clock step never fires an event twice.
   - **Late events:** each event has a `late_policy`. `fire_late` fires immediately and reports the lateness (the default); `drop_late` drops the event when it is later than a tolerance.

5. **Timeline synchronization:**
   - A timeline is state, not a stream of events: `{timeline_id, version, T0, P0, rate}`. Every node computes `position(now) = P0 + (shared_now - T0) × rate` locally.
   - Play, pause, seek and rate changes are `TIMELINE` messages that take effect at a future shared time, just like events.
   - For visuals, apps should compute the timeline position from shared time in `draw()` rather than waiting for events.
   - A node that joins late receives a `SNAPSHOT` of every timeline and pending event from the master.

6. **Dispatch:** A dedicated scheduler thread sleeps with `clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME)` until the next deadline. Every callback receives the event plus `lateness_us`. Two ways to receive events:
   - **Thread callback:** for GPIO or audio. It has the tightest timing, and the callback must be thread-safe.
   - **Main-thread queue:** drained in `update()`. It can be up to a frame late, and `lateness_us` lets animations correct for that.

7. **Continuous adaptation:**
   - The ongoing pings keep the offset current.
   - A linear fit of offset over the window (minutes long) estimates skew. A Pi crystal drifts by tens of ppm, roughly 50–150 ms per hour if uncorrected.
   - Corrections are slewed (see Clock).

## Configuration

The addon takes a config struct, and the examples fill it from `bin/data/settings.xml`:
- `group`, `node_id`, `priority`, `interface`, `peers` (static list), `discovery` (`static` | `beacon` | `bonjour`), `port`
- `ping_hz`, `lead_floor_ms`, `late_tolerance_ms`, `step_threshold_ms`, `output_latency_us`
- `auth_token` (WebSocket only)

## Examples

Once the core logic is working, create two examples for the addon. The OSC example is built alongside the core, because sync can't be tested without a transport.

### OSC example (fast, local-network UDP synchronization)
- Uses `ofxOsc` only, which ships with openFrameworks.
- Receive timestamps come from a subclass of `ofxOscReceiver` that overrides the `virtual protected ProcessMessage`, which runs on the receiver's listener thread.
- On screen: role, term, shared clock, estimated error, delay and drift for each peer.
- A test pattern: a scheduled flash every second, with an optional GPIO pulse, and a timeline-driven moving bar.
- Keys: schedule an event, play/pause/seek the timeline, kill the master (to test failover).

### WebSocket example (TCP-based synchronization)
- **Master:** runs an `ofxHTTP::SimpleWebSocketServer`, set up the same way as in the reference app, including the raised frame buffer size:
  `~/openFrameworks/of_v0.12.1_linuxaarch64_release/apps/myApps/PiNaplpsPlayer/` (`src/Pinopticon_Http.hpp`, `setupWsServer`)
- **Followers:** a Pi WebSocket client built on `Poco::Net::WebSocket` (a commented-out sketch is in `Pinopticon_Http.hpp`), running on its own thread with reconnect and backoff, and with `setNoDelay(true)` to avoid Nagle delays.
- **Optional:** a minimal browser client page that shows the shared clock. Browser timers are coarse, so it is a demo, not a precision client.
- **External access:** so the Pi can be contacted externally.
  - **Recommended:** Tailscale, already on the dev Pi. It needs no port forwarding and is encrypted and access-controlled.
  - **Alternative:** port forwarding with `wss://` through `ofxSSLManager`, since browsers on HTTPS pages refuse `ws://`.
  - **Either way,** clients must present `auth_token` before they can schedule events or change timelines.
- **Expectations:** sync is looser than on the LAN. The example shows estimated error and the automatically scaled lead time.
- **Build:** `ofxPoco`'s `addon_config.mk` has no `linuxaarch64` section. The example ships a `setup.sh` that applies the same idempotent patch as PiNaplpsPlayer's. The core and the OSC example don't need it.

## Verification

- **Unit tests** (`ofxUnitTests`) for the offset math, sample filtering, slewing and step behavior, de-duplication, timeline position and master selection. They run over an in-process loopback transport with simulated delay, jitter and loss.
- **Hardware measurement:** every node pulses a GPIO pin at the same scheduled times, and the skew is measured with a logic analyzer or scope. A screen flash measured with a photodiode or high-speed camera measures the full path including the display.
- **Network impairment:** `tc qdisc add dev eth0 root netem delay 20ms 10ms loss 5%` to test jitter and loss handling.
- **Scenario tests:** failover (kill the master), a node joining late, a network split and heal, and an 8+ hour run to test drift.
- **CSV logging** of offset, delay, error and drift for each peer.

## Raspberry Pi setup notes (for the README)

- Prefer wired Ethernet, and set `interface` to it.
- Turn off WiFi power saving; it adds tens to hundreds of ms of jitter. Run `sudo iw wlan0 set power_save off`, or make it persistent with `nmcli connection modify <conn> 802-11-wireless.powersave 2`.
- `systemd-timesyncd` can stay on. The addon doesn't use the system clock.

## Order of work

1. Clock, message types and the loopback transport, with unit tests.
2. The OSC adapter and a minimal OSC example: two nodes, a static peer list and a configured master. Measure against the targets.
3. The scheduler and dispatch, events with acks and de-duplication, and timelines with snapshots.
4. Discovery backends, master selection and failover.
5. The WebSocket adapter and example, with external access and auth.
6. README, including the Pi setup notes.
