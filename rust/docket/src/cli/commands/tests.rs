use std::time::Duration;

use super::render_workers;
use crate::Timestamp;
use crate::docket::WorkerSummary;

#[test]
fn workers_list_the_most_recently_seen_first() {
    let now = Timestamp::from_unix_micros(1_800_000_000_000_000).unwrap();
    let worker = |name: &str, seconds: u64, tasks: &[&str]| WorkerSummary {
        name: name.to_owned(),
        last_seen: now - Duration::from_secs(seconds),
        tasks: tasks.iter().map(|&task| task.to_owned()).collect(),
    };
    let workers = [worker("old", 9, &[]), worker("new", 1, &["a", "b"])];
    assert_eq!(
        render_workers("d", now, &workers),
        concat!(
            "     Workers in Docket: d      \n",
            "┏━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━┓\n",
            "┃ Name ┃ Last Seen   ┃ Tasks  ┃\n",
            "┡━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━┩\n",
            "│ new  │ 0:00:01 ago │ a      │\n",
            "│      │             │ b      │\n",
            "│ old  │ 0:00:09 ago │ (none) │\n",
            "└──────┴─────────────┴────────┘\n",
        )
    );
}
