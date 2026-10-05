use std::collections::BTreeSet;

use super::ALL;

#[test]
fn every_declaration_matches_its_script_header() {
    for (file, slots, source) in ALL {
        let header = slots.header();
        assert!(
            source.starts_with(&format!("{header}\n\n")),
            "{file}.lua must start with exactly these lines, then a blank line:\n{header}"
        );
    }
}

#[test]
fn every_protocol_script_is_declared() {
    let declared: BTreeSet<&str> = ALL.iter().map(|(file, _, _)| *file).collect();
    let shipped: BTreeSet<String> = std::fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/lua"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| path.file_stem().unwrap().to_string_lossy().into_owned())
        .collect();
    assert_eq!(declared, shipped.iter().map(String::as_str).collect());
}
