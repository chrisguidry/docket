//! Moves a slot between the nodes of the test cluster, the way a cluster
//! operator rebalances one, so tests can see what docket does when its
//! connections still route by the old slot map.

use redis::aio::MultiplexedConnection;
use redis::{Client, cmd};
use tokio::sync::Mutex;

/// Moves one slot at a time.  Moves that overlapped left a node redirecting
/// to the slot's old owner now and then, likely because each move bumps the
/// nodes' config epochs and the nodes settle colliding epochs by gossip.
static MOVING: Mutex<()> = Mutex::const_new(());

/// The test suite's cluster URL, or `None` when the suite runs against
/// anything else.
pub(crate) fn cluster_url() -> Option<String> {
    std::env::var("DOCKET_TEST_URL")
        .ok()
        .filter(|url| url.starts_with("redis+cluster://"))
}

/// One node of the cluster, from a line of `CLUSTER NODES`.
struct Node {
    id: String,
    address: String,
    slots: Vec<(u16, u16)>,
}

impl Node {
    fn parse(line: &str) -> Self {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let address = fields[1].split('@').next().unwrap_or_default().to_owned();
        // A slot in the middle of a move shows up as `[slot->-id]`.
        let slots = fields[8..]
            .iter()
            .filter(|range| !range.starts_with('['))
            .map(|range| {
                let (first, last) = range.split_once('-').unwrap_or((range, range));
                (first.parse().unwrap(), last.parse().unwrap())
            })
            .collect();
        Self {
            id: fields[0].to_owned(),
            address,
            slots,
        }
    }

    fn owns(&self, slot: u16) -> bool {
        self.slots
            .iter()
            .any(|(first, last)| (*first..=*last).contains(&slot))
    }

    async fn connect(&self) -> MultiplexedConnection {
        Client::open(format!("redis://{}", self.address))
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap()
    }
}

/// Moves the slot of `key`, with any keys in it, from the node that owns it
/// to another node.
pub(crate) async fn move_slot(cluster_url: &str, key: &str) {
    let _moving = MOVING.lock().await;
    let seed = cluster_url.replacen("redis+cluster://", "redis://", 1);
    let mut seed = Client::open(seed)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let slot: u16 = cmd("CLUSTER")
        .arg("KEYSLOT")
        .arg(key)
        .query_async(&mut seed)
        .await
        .unwrap();
    let listing: String = cmd("CLUSTER")
        .arg("NODES")
        .query_async(&mut seed)
        .await
        .unwrap();
    let nodes: Vec<Node> = listing.lines().map(Node::parse).collect();
    let source = nodes.iter().find(|node| node.owns(slot)).unwrap();
    let target = nodes.iter().find(|node| !node.owns(slot)).unwrap();
    let mut from = source.connect().await;
    let mut to = target.connect().await;

    let setslot = |state: &str, node: &Node| {
        let mut command = cmd("CLUSTER");
        command.arg("SETSLOT").arg(slot).arg(state).arg(&node.id);
        command
    };
    let () = setslot("IMPORTING", source)
        .query_async(&mut to)
        .await
        .unwrap();
    let () = setslot("MIGRATING", target)
        .query_async(&mut from)
        .await
        .unwrap();
    let (host, port) = target.address.rsplit_once(':').unwrap();
    loop {
        let keys: Vec<String> = cmd("CLUSTER")
            .arg("GETKEYSINSLOT")
            .arg(slot)
            .arg(100)
            .query_async(&mut from)
            .await
            .unwrap();
        if keys.is_empty() {
            break;
        }
        let () = cmd("MIGRATE")
            .arg(host)
            .arg(port)
            .arg("")
            .arg(0)
            .arg(5000)
            .arg("KEYS")
            .arg(&keys)
            .query_async(&mut from)
            .await
            .unwrap();
    }
    // The target learns that it owns the slot before anyone else, so that
    // a redirect never points at a node that refuses the slot.
    for node in std::iter::once(target).chain(nodes.iter().filter(|node| node.id != target.id)) {
        let () = setslot("NODE", target)
            .query_async(&mut node.connect().await)
            .await
            .unwrap();
    }
}
