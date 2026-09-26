- This is an openFrameworks addon for use with Raspberry Pis to let them synchronize events and timelines over a network.

- It will be a set of methods independent of transport protocol, allowing it to be used interchangeably with OSC, WebSockets, or other network protocols.

- The synchronization process will operate by establishing a shared time base:
1. **Discovery:** Dynamically discover other nodes on the local network using Bonjour (mDNS) by broadcasting/listening for a specific service type, or optionally resolve specific Bonjour hostnames listed in a `settings.xml` file for predefined network topologies.
2. **Clock Synchronization:** Establish a reliable, shared internal clock across all nodes. The nodes will exchange timestamped "heartbeat" messages to calculate network latency (round-trip time) and offset, adjusting their local clock to align with a chosen master node.
3. **Event Scheduling:** To perform synchronized actions, nodes will not rely on instantaneous message delivery. Instead, the master will send instructions to "execute event X at future synchronized time T". Because all nodes share the same logical clock, they will trigger the action locally at the exact same moment, mitigating network jitter.
4. **Continuous Adaptation:** At a lower frequency, nodes will continue to monitor network latency and time drift, making micro-adjustments to the clock offset to maintain high-precision synchronization over long durations.

Once this core logic is complete, create two examples for the addon:
- An **OSC example** demonstrating fast, local network UDP synchronization.
- A **WebSocket example** demonstrating TCP-based synchronization. For this example, set up a server so that the Pi can be contacted externally. For server addons and implementation reference, see:
`~/openFrameworks/of_v0.12.1_linuxaarch64_release/apps/myApps/PiNaplpsPlayer/`
