# P2P Chat using Rust & libp2p

This is a demostration of peer-to-peer (P2P) gossip protocall built in Rust using the `libp2p` framework. It does not rely on any central server. Instead, it uses **mDNS** to automatically discover other users on your local network and the **Gossipsub** protocol to broadcast messages securely between peers.

## Getting Started

### 1. Prerequisites

To run this project, you need to have **Rust** installed on your machine.

- **Rust** (stable, 1.75+): [https://rustup.rs](https://rustup.rs)

```powershell
rustup update stable
```

## Installation & Build

```powershell
git clone https://github.com/codeArray-go/p2p_gossip.rs.git p2p_chat

cd "p2p_chat"

cargo build
```

### 3. How to Run

Because this is a peer-to-peer network, running just one instance isn't very fun, you need someone to talk to!

1. Open a terminal and run the first node:

```bash
RUST_LOG=info cargo run

```

2. Open a **second, separate terminal window** and run another node:

```bash
RUST_LOG=info cargo run

```

_Note: `RUST_LOG=info` enables the tracing logs so you can see background events if needed._

**What to expect:**

- Both terminals will print `Local node is listening on...`
- Within a few seconds, mDNS will find the other node. You will see `mDNS discovered a new peer: [PeerId]`.
- Type a message into Terminal 1 and press Enter. It will instantly appear in Terminal 2.

---

## How it Works: Explanation

Here is a step-by-step breakdown of every major component in the `src/main.rs` file.

### 1. The Network Behaviour

In `libp2p`, a "Behaviour" dictates how your node reacts to the network. By deriving `NetworkBehaviour` on a custom struct, we combine two distinct protocols:

- **`gossipsub`**: The chat room protocol. It handles subscribing to topics and routing messages efficiently so they don't flood the network.
- **`mdns`**: Multicast DNS. It acts as a local radar, broadcasting your presence to the local WiFi/network to find other peers automatically.

### 2. Building the Swarm (The P2P Engine)

The `Swarm` is the core orchestrator of a `libp2p` node. It manages your cryptographic keys, network connections, and the custom behaviors defined above.

- **`SwarmBuilder::with_new_identity()`**: Generates a fresh, unique cryptographic keypair for your node every time it boots up.
- **`.with_tokio()`**: Tells the network to use the Tokio asynchronous runtime to manage background tasks.
- **`.with_tcp(...)` & `.with_quic()`**: These configure the transport layers.
- **TCP** is wrapped in `noise` (for end-to-end encryption) and `yamux` (for stream multiplexing, allowing multiple simultaneous data streams over one connection).
- **QUIC** is built on UDP and natively supports encryption and multiplexing, making it incredibly fast.

### 3. Configuring Gossipsub

Inside the `.with_behaviour(...)` closure, we configure how messages are handled:

- **`message_id_fn`**: We create a custom closure that takes the raw bytes of a message and hashes them using Rust's `DefaultHasher`. We use this hash as the unique `MessageId`. This ensures that if the exact same message is routed through multiple peers, the network recognizes it as a duplicate and doesn't process it twice.
- **`heartbeat_interval`**: Set to 10 seconds. This is how often Gossipsub performs network maintenance (like pruning dead connections).
- **`ValidationMode::Strict`**: Ensures every message is cryptographically signed by the sender's private key, preventing spoofing.

### 4. Topic Subscription & Input Handling

```rust
let topic = gossipsub::IdentTopic::new("test-net");
swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

```

Gossipsub uses a Publish/Subscribe (PubSub) model. We define a topic called `"test-net"` and tell our node to subscribe to it. Peers will only see messages if they are subscribed to the exact same topic string.

```rust
let mut stdin = io::BufReader::new(io::stdin()).lines();

```

This efficiently wraps your terminal input (Standard In) so the program can asynchronously read everything you type line-by-line without freezing the rest of the network engine.

### 5. Starting the Listeners

```rust
swarm.listen_on("/ip4/0.0.0.0/udp/0/quic-v1".parse()?)?;
swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

```

We tell the node to open network sockets to listen for incoming connections.

- `/ip4/0.0.0.0`: Listen on all available local network interfaces (like WiFi and Ethernet).
- `/0`: The zero means "let the Operating System pick a random available port." This prevents port conflicts when running multiple nodes on the same computer.

### 6. The Main Event Loop

The `loop { select! { ... } }` block is the heart of the application. It constantly waits for one of two things to happen concurrently:

**Branch A: You type a message (`stdin.next_line()`)**
When you type a line and press enter, the code takes that `String`, converts it into raw bytes (`line.as_bytes()`), and commands Gossipsub to `.publish()` it to the `"test-net"` topic.

**Branch B: A network event occurs (`swarm.select_next_some()`)**
When the Swarm emits an event, we use pattern matching (`match event`) to react:

- **`mdns::Event::Discovered`**: The radar found a new peer! We print their ID and explicitly add them to our Gossipsub routing table.
- **`mdns::Event::Expired`**: A peer disconnected or turned off their computer. We remove them from our routing table.
- **`gossipsub::Event::Message`**: Someone on the network published a message to `"test-net"`. We extract the raw data bytes, convert them back into a readable UTF-8 string (`String::from_utf8_lossy`), and print it to the screen along with the sender's ID.
- **`SwarmEvent::NewListenAddr`**: Fires when the OS successfully assigns our random ports, printing out our node's local address.
