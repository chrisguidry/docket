use chrono::{DateTime, TimeDelta, Utc};
use rstest::rstest;
use serde_json::json;

use super::{Justify, Table, local_time, relative_time, repr, repr_str, timedelta};

/// rich's own output for the same tables, with `Console(color_system=None)`.
#[test]
fn an_empty_table_matches_rich() {
    let table = Table::new("Docket: 'x'\nas of 2026\n0 workers, 0/0 running")
        .column("When", Justify::Left)
        .column("Function", Justify::Left)
        .column("Total", Justify::Right);
    assert_eq!(
        table.render(),
        concat!(
            "        Docket: 'x'        \n",
            "        as of 2026         \n",
            "  0 workers, 0/0 running   \n",
            "┏━━━━━━┳━━━━━━━━━━┳━━━━━━━┓\n",
            "┃ When ┃ Function ┃ Total ┃\n",
            "┡━━━━━━╇━━━━━━━━━━╇━━━━━━━┩\n",
            "└──────┴──────────┴───────┘\n",
        )
    );
}

#[test]
fn a_table_with_rows_matches_rich() {
    let mut table = Table::new("Workers in Docket: d")
        .column("Name", Justify::Left)
        .column("Last Seen", Justify::Left)
        .column("Total", Justify::Right);
    table.row(vec!["host#1".into(), "0:00:01 ago".into(), "1".into()]);
    table.row(vec![
        "a-much-longer-name".into(),
        "x".into(),
        "12345\n2".into(),
    ]);
    assert_eq!(
        table.render(),
        concat!(
            "            Workers in Docket: d            \n",
            "┏━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━┓\n",
            "┃ Name               ┃ Last Seen   ┃ Total ┃\n",
            "┡━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━┩\n",
            "│ host#1             │ 0:00:01 ago │     1 │\n",
            "│ a-much-longer-name │ x           │ 12345 │\n",
            "│                    │             │     2 │\n",
            "└────────────────────┴─────────────┴───────┘\n",
        )
    );
}

#[test]
fn a_row_of_empty_cells_takes_one_line() {
    let mut table = Table::new("t").column("A", Justify::Left);
    table.row(vec![String::new()]);
    assert!(table.render().contains("│   │\n"));
}

#[test]
fn a_title_wider_than_its_table_wraps() {
    let table = Table::new("one two threefourfivesix").column("A", Justify::Left);
    let rendered = table.render();
    let lines: Vec<&str> = rendered.lines().take(5).collect();
    assert_eq!(lines, [" one ", " two ", "three", "fourf", "ivesi"]);
}

fn at(seconds: i64, micros: u32) -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000 + seconds, micros * 1000).unwrap()
}

#[rstest]
#[case::zero(TimeDelta::zero(), "0:00:00")]
#[case::fraction(TimeDelta::microseconds(5_250_000), "0:00:05.250000")]
#[case::hours(TimeDelta::seconds(3723), "1:02:03")]
#[case::one_day(TimeDelta::days(1), "1 day, 0:00:00")]
#[case::days(TimeDelta::days(2) + TimeDelta::microseconds(5), "2 days, 0:00:00.000005")]
#[case::negative(TimeDelta::seconds(-1), "-1 day, 23:59:59")]
#[case::negative_days(TimeDelta::days(-3), "-3 days, 0:00:00")]
fn durations_print_as_python_timedeltas(#[case] delta: TimeDelta, #[case] expected: &str) {
    assert_eq!(timedelta(delta), expected);
}

#[test]
fn relative_times_match_pydocket() {
    let now = at(0, 0);
    assert_eq!(relative_time(now, at(-5, 0)), "0:00:05 ago");
    assert_eq!(relative_time(now, at(90, 500)), "in 0:01:30.000500");
    assert_eq!(
        relative_time(now, at(-1800, 0)),
        format!("at {}", local_time(at(-1800, 0)))
    );
    assert_eq!(
        relative_time(now, at(1801, 0)),
        format!("at {}", local_time(at(1801, 0)))
    );
    assert_eq!(relative_time(now, at(1800, 0)), "in 0:30:00");
}

#[test]
fn local_times_carry_their_offset() {
    let text = local_time(at(0, 0));
    let (_, offset) = text.rsplit_once(' ').unwrap();
    assert_eq!(text.len(), "2027-01-15 08:00:00 +0000".len(), "{text}");
    assert!(offset.starts_with(['+', '-']), "{text}");
}

#[rstest]
#[case::plain("abc", "'abc'")]
#[case::single_quote("it's", "\"it's\"")]
#[case::double_quote("a\"b", "'a\"b'")]
#[case::both_quotes("a'b\"c", "'a\\'b\"c'")]
#[case::escapes("x\\y\n\r\t", "'x\\\\y\\n\\r\\t'")]
fn strings_print_as_python_reprs(#[case] text: &str, #[case] expected: &str) {
    assert_eq!(repr_str(text), expected);
}

#[test]
fn values_print_as_python_reprs() {
    assert_eq!(
        repr(&json!([null, true, false, 7, 2.5, "s", {"k": [1]}])),
        "[None, True, False, 7, 2.5, 's', {'k': [1]}]"
    );
}
