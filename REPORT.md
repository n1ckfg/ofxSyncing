# ofxSyncing: implementation report

2026-09-27. Built and tested on a Raspberry Pi 4 (Pi OS Bookworm, 64-bit) with openFrameworks 0.12.1 linuxaarch64.

## Summary

Everything in PLAN.md is implemented: the core, both transports, all three discovery backends, both examples, a unit-test app and a README. All 125 unit tests pass, and the core, both examples and the tests build without warnings from the addon's own code.

Everything was verified on one Pi, in simulation and with separate processes. The precision between separate Pis still has to be measured on real hardware (see [Not yet verified](#not-yet-verified)).

## What was built

| Path | |
|---|---|
| `src/` | The core, depending only on the openFrameworks core: clock and sync estimator, master selection with failover and split/heal, events with acks, resends and de-duplication, timelines with snapshots for late joiners, a `clock_nanosleep` scheduler thread, CSV logging, and a simulated in-process network (`ofxSyncingLoopback.h`) for the tests |
| `src/ofxSyncingOsc.h` | OSC transport, header-only (needs `ofxOsc`) |
| `src/ofxSyncingWebSocket.h` | WebSocket transport, header-only (needs `ofxHTTP` and its addons): server for the master, client with reconnect and backoff for followers, token auth |
| `src/ofxSyncingDiscovery.*`, `src/ofxSyncingAvahi.h` | Discovery: multicast beacons (the default), a static peer list, and optional Bonjour through Avahi |
| `example_osc/` | Status display, a flash every second, a timeline-driven bar, GPIO pulses, and a key that kills and revives the node |
| `example_websocket/` | One app that runs as server or client, `setup.sh`, and a browser client in `bin/data/DocumentRoot` |
| `tests/` | ofxUnitTests app: 125 tests, about 11 s |
| `README.md` | Usage, configuration, transports, discovery, verification, and the Pi setup notes |

## Results

### Unit tests

Most tests run on the simulated network, which models delay, jitter, loss and one-way spikes, and gives every node its own drifting crystal. A seed makes each run the same. The last test uses real UDP sockets and threads.

| Scenario | Spread between clocks (p95) |
|---|---|
| Wired LAN, simulated (150 ± 50 µs, crystals up to ±110 ppm) | 0.03 ms |
| WiFi-like, simulated (2 ± 1 ms, 1% loss, 20% of packets spiking up to 40 ms) | 0.46 ms |
| `netem delay 20ms 10ms loss 5%`, simulated | 3.7 ms |
| 8 hours of drift, crystals 200 ppm apart, simulated in about 3 s | 0.03 ms (max 0.05 ms) |
| Two processes on one Pi, real UDP | 0.07–0.13 ms |

These targets from the plan are met:

| Target | Result |
|---|---|
| Wired Ethernet, ±1 ms | 0.03 ms simulated; 0.07–0.13 ms between processes on one Pi |
| WiFi with power saving off, ±5 ms | 0.46 ms simulated; 3.7 ms under heavy netem impairment |
| Events fire within 1 ms of their time | 0.06–0.11 ms late on the scheduler thread, and within 0.07–0.12 ms of each other across two processes |

The tests also cover: offset math, sample filtering, the drift fit, slewing and stepping, both wire formats, master selection, failover with a pending event surviving the master, a returning node (including one that remembers a later term), a node joining late, a network split and heal, late policies, output latency, de-duplication across resends and clock steps, and follower requests under 20% loss.

### Live runs on one Pi

- **OSC, multicast discovery:** two nodes found each other and locked with a 0.2 ms round trip. Every tick fired on both, 13–63 µs after its time.
- **OSC failover:** killing the master, the follower noticed after about 3 s and took over with term 2, and the ticks carried on. The revived node came back as a follower, and both read the same timeline position (22.633 and 22.632 s, sampled a moment apart).
- **OSC on eth0:** with `interface` set, the nodes bound to eth0's address, warned that wlan0 shares its subnet, and locked with a 0.11 ms round trip.
- **Bonjour:** two nodes found each other through avahi-daemon and locked with a 0.087 ms round trip.
- **WebSocket:** a client authorized and locked with a 0.63 ms median round trip (p99 1.5 ms) and a 0.18 ms error estimate. The server served the browser client page.
- **WebSocket restart:** with the server restarted under a live client, the client noticed after 3 s, reconnected, re-authorized and followed the new master, locked with a 0.19 ms error estimate.
- **WebSocket security**, from a scripted client: a wrong token was refused, event requests and timeline changes from the unauthorized client were dropped, and a fake master claim at term 999 was ignored.

## Problems found and fixed

- **ofxHTTP frames:** `WebSocketFrame::toString()` returns a description of the frame's flags, not its text, so at first the server ignored every client frame. The adapter now reads the payload bytes directly.
- **ofxHTTP poll timeout:** the connection loop blocks for its full poll timeout (10 ms by default) on every turn, so an incoming PING can wait before it's read. That skewed clock samples. With the adapter's timeout at 1 ms, round trips on one machine dropped from 4–6 ms to 0.6 ms.
- **Stale samples:** before the drift was known, the estimator used samples too old to trust, which cost up to about 0.6 ms at 80 ppm. It now weighs each sample NTP-style (half its delay plus its age times the drift uncertainty) and fits the drift early when the fit is tight. That took agreement between two processes on one Pi from 0.36 ms to about 0.1 ms, with the drift known 5 s after locking.
- **Returning node with a later term:** a node that had elected itself while cut off would have taken over from the current master when it came back. It now follows. The new test fails without the fix.
- **Reordered beacons:** a master's beacon from before it was promoted, arriving late, would have unseated it. It's now ignored.
- **Clock steps:** events left over from an old timebase after a large step no longer fire minutes late.
- **Boot order:** multicast discovery keeps retrying until the network is up, so an app started at boot finds its peers.
- **Headless apps:** oF complained that `ofEvents()` was called before a window existed, because ofxHTTP's constructors use it. The examples now register the headless window before constructing the app.

## Additions to the plan

- Timeline requests from followers are `TIMELINE` messages with version 0; the plan's table had no request type.
- Beacons also carry each node's sync stats, shared time and lead time, so every node can show the whole group.
- `PING` carries a `want_snapshot` flag, so a late joiner asks for the snapshot until it arrives.
- A master that never locked starts from a peer's shared time, so its timeline doesn't jump.
- On the WebSocket server, messages meant for one client go out as broadcasts addressed by node id (`"to"`), because ofxHTTP only queues sends and a connection object can't safely be used from another thread. A PONG is sent from inside that connection's own callback, so it goes out at once.
- The examples run headless when there's no display, reading keys from stdin.
- `example_websocket/setup.sh` also applies PiNaplpsPlayer's `#include <memory>` patch to `ofTypes.h`, alongside the ofxPoco patch.

## Not yet verified

These need hardware or setups this Pi doesn't have:

- **Precision between separate Pis**, wired and over WiFi, measured with GPIO pulses and a logic analyzer (the method is in the README). All of the numbers above come from one Pi.
- **A real GPIO pin.** The pulse code builds, but no pin was toggled, since what's wired to this Pi is unknown.
- **Windowed mode.** This Pi has no display.
- **`wss://` with a real certificate**, and **Tailscale between machines**.
- **Avahi with the system package.** It was tested with `libavahi-client-dev`'s headers unpacked into a temporary folder, since the package isn't installed here.

## Notes

- The "build" commit includes the three compiled binaries (`example_osc/bin/example_osc`, `example_websocket/bin/example_websocket`, `tests/bin/tests`). The template's `[Bb]in/*` rule in `.gitignore` only matches a top-level `bin/`.
- To rerun the tests: `cd tests && make && ./bin/tests`. The exit code is the number of failures.
