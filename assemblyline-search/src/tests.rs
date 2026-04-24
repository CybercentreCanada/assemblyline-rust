
use chrono::{Utc, Duration};
use serde_json::json;


use crate::json::{AllFields, JsonFilter};
use crate::lucene::Query;


#[test]
fn test_simple_filters() {

    let mut sub = json!({"max_score": 100, "times": {"completed": Utc::now()}});

    // Build our test filter
    let fltr = Query::parse("times.completed: [now-1d TO 2055-06-20T10:10:10.000] AND max_score: [10 TO 100]").unwrap();

    // Make sure it works for our starting test data
    assert!(AllFields::test(&fltr, &sub).unwrap());

    // Test some different values
    sub.as_object_mut().unwrap().insert("max_score".to_owned(), json!(101));
    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub.as_object_mut().unwrap().insert("max_score".to_owned(), json!(11));
    assert!(AllFields::test(&fltr, &sub).unwrap(), "{sub}");
    sub = json!({"max_score": 11, "times": {"completed": Utc::now() - Duration::try_days(2).unwrap()}});
    assert!(!AllFields::test(&fltr, &sub).unwrap());

    // Ranges with numbers and strings
    let fltr = Query::parse("max_score: {10 TO 100} OR metadata.stuff: {\"big cats\" TO cats}").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["max_score"], vec!["metadata", "stuff"]]);

    assert!(AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"max_score": 10, "times": {"completed": Utc::now() - Duration::try_days(2).unwrap()}});
    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"max_score": 10, "metadata": {"stuff": "cats"}});
    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"max_score": 10, "metadata": {"stuff": "big dogs"}});
    assert!(AllFields::test(&fltr, &sub).unwrap(), "{sub} {fltr:?}");
    sub = json!({"max_score": 10, "metadata": {"stuff": "aig dogs"}});
    assert!(!AllFields::test(&fltr, &sub).unwrap());

    // Try a prefix operator and wildcard matches
    let fltr = Query::parse("max_score: >100 AND NOT results: *virus*").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["max_score"], vec!["results"]]);

    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"max_score": 101, "results": ["a-virus-service"]});
    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"max_score": 101, "results": ["a-something-service"]});
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("max_score: >100 AND results: *virus*").unwrap();
    sub = json!({"max_score": 101, "results": ["a-virus-service"]});
    assert!(AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"max_score": 101, "results": ["a-something-service"]});
    assert!(!AllFields::test(&fltr, &sub).unwrap());

    // different prefix operator
    let fltr = Query::parse("files.size:>100").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["files", "size"]]);

    sub = json!({"files": []});
    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"files": [{"name": "abc", "size": 100, "sha256": "0".repeat(64)}]});
    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"files": [{"name": "abc", "size": 100, "sha256": "0".repeat(64)}, {"name": "abc", "size": 101, "sha256": "0".repeat(64)}]});
    assert!(AllFields::test(&fltr, &sub).unwrap());

    // Include logic within a field, the cats should be a global search, and hit on the description
    let fltr = Query::parse("metadata.stuff: (things OR stuff) AND cats").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec![], vec!["metadata", "stuff"]]);

    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"params": {"description": "Full of cats."}});
    assert!(!AllFields::test(&fltr, &sub).unwrap());
    sub = json!({"params": {"description": "Full of cats."}, "metadata": {"stuff": "things"}});
    assert!(AllFields::test(&fltr, &sub).unwrap(), "{sub} {fltr:?}");

    // Try a bunch of different ways to format the same thing
    sub = json!({"metadata": {"stuff": "big-bad"}});
    let fltr = Query::parse("metadata.stuff: \"big-bad\"").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["metadata", "stuff"]]);
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("metadata.stuff: big-bad").unwrap();
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("metadata.stuff: big-*").unwrap();
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("metadata.stuff: big-???").unwrap();
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("metadata.stuff: big\\-bad").unwrap();
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("metadata.stuff: \"big bad\"").unwrap();
    sub = json!({"metadata": {"stuff": "big bad"}});
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("metadata.stuff: big\\ bad").unwrap();
    assert!(AllFields::test(&fltr, &sub).unwrap());
}


#[test]
fn test_date_boundaries() {

    let sub_below = json!({"times": { "completed": "2054-06-20T10:10:11.049Z" }});
    let sub_low = json!({"times": { "completed": "2054-06-20T10:10:11.050Z" }});
    let sub_inside = json!({"times": { "completed": "2054-07-20T10:10:10.000Z" }});
    let sub_high = json!({"times": { "completed": "2055-06-20T10:10:10.000Z" }});
    let sub_over = json!({"times": { "completed": "2055-06-20T10:10:10.001Z" }});

    // Build our test filter
    let fltr = Query::parse("times.completed: [2054-06-20T10:10:11.050Z TO 2055-06-20T10:10:10.000Z]").unwrap();
    assert!(!AllFields::test(&fltr, &sub_below).unwrap());
    assert!(AllFields::test(&fltr, &sub_low).unwrap());
    assert!(AllFields::test(&fltr, &sub_inside).unwrap());
    assert!(AllFields::test(&fltr, &sub_high).unwrap());
    assert!(!AllFields::test(&fltr, &sub_over).unwrap());

    let fltr = Query::parse("times.completed: {2054-06-20T10:10:11.050 TO 2055-06-20T10:10:10.000}").unwrap();
    assert!(!AllFields::test(&fltr, &sub_below).unwrap());
    assert!(!AllFields::test(&fltr, &sub_low).unwrap());
    assert!(AllFields::test(&fltr, &sub_inside).unwrap());
    assert!(!AllFields::test(&fltr, &sub_high).unwrap());
    assert!(!AllFields::test(&fltr, &sub_over).unwrap());
}


#[test]
fn test_tag_filters() {
    let sub = json!({"max_score": 100});
    let sub_tags = json!({
        "max_score": 100,
        "tags": {
            "vector": ["things"],
            "technique": {"packer": ["giftwrap"]},
        }
    });
    // tags = [
    //     {'safelisted': False, 'type': '', 'value': 'things', 'short_type': 'vector'},
    //     {'safelisted': False, 'type': '', 'value': 'giftwrap', 'short_type': 'packer'}
    // ];

    let fltr = Query::parse("max_score: >=100 AND tags.vector: *").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["max_score"], vec!["tags", "vector"]]);

    assert!(!AllFields::test(&fltr, &sub).unwrap());
    assert!(AllFields::test(&fltr, &sub_tags).unwrap());

    let fltr = Query::parse("max_score: >=100 AND things").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec![], vec!["max_score"]]);

    assert!(!AllFields::test(&fltr, &sub).unwrap());
    assert!(AllFields::test(&fltr, &sub_tags).unwrap());

    let fltr = Query::parse("max_score: >=100 AND things AND giftwrap").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec![], vec!["max_score"]]);

    assert!(!AllFields::test(&fltr, &sub).unwrap());
    assert!(AllFields::test(&fltr, &sub_tags).unwrap());

    let fltr = Query::parse("max_score: >=100 AND tags.technique.packer: *wrap").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["max_score"], vec!["tags", "technique", "packer"]]);

    assert!(!AllFields::test(&fltr, &sub).unwrap());
    assert!(AllFields::test(&fltr, &sub_tags).unwrap());
}

#[test]
fn test_regex_filter() {
    let sub = json!({});

    let fltr = Query::parse("metadata.other: /ab+c/").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["metadata", "other"]]);

    assert!(!AllFields::test(&fltr, &sub).unwrap());

    let sub = json!({"metadata": {"other": "ac"}});
    assert!(!AllFields::test(&fltr, &sub).unwrap());

    let sub = json!({"metadata": {"other": "abbbc"}});
    assert!(AllFields::test(&fltr, &sub).unwrap());
}

#[test]
fn test_exists() {

    let fltr = Query::parse("_exists_: metadata.other").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["metadata", "other"]]);

    let fltr = Query::parse("_exists_: nteohusotehuos").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["nteohusotehuos"]]);

    let fltr = Query::parse("cats OR _exists_: nteohusotehuos").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec![], vec!["nteohusotehuos"]]);

    let sub = json!({
        "metadata": {
            "cats": "good"
        }
    });

    let fltr = Query::parse("_exists_: metadata.other").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["metadata", "other"]]);
    assert!(!AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("NOT _exists_: metadata.other").unwrap();
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("_exists_: metadata.cats").unwrap();
    assert!(AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("NOT _exists_: metadata.cats").unwrap();
    assert!(!AllFields::test(&fltr, &sub).unwrap());
}

#[test]
fn test_date_truncate() {
    let fltr = Query::parse("times.completed: [2020-08-08T10:10:10.000Z TO 2020-08-09T10:10:10.000Z]").unwrap();
    let sub = json!({
        "times": {
            "completed": "2020-08-08T09:10:10.000Z"
        }
    });
    assert!(!AllFields::test(&fltr, &sub).unwrap());

    let fltr = Query::parse("times.completed: [2020-08-08T10:10:10.000Z||/d TO 2020-08-09T10:10:10.000Z]").unwrap();
    assert!(AllFields::test(&fltr, &sub).unwrap());
}

#[test]
fn test_subobject_filters() {
    let document = json!({
        "tags": [
            {
                "name": "vector",
                "value": "things",
            },
            {
                "name": "technique.packer",
                "value": "giftwrap",
            }
        ]
    });


    let fltr = Query::parse("tags.name: vector AND tags.value: giftwrap").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["tags", "name"], vec!["tags", "value"]]);
    assert!(AllFields::test(&fltr, &document).unwrap());

    let fltr = Query::parse("{tags.name: vector AND tags.value: giftwrap}").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["tags", "name"], vec!["tags", "value"]]);
    assert!(!AllFields::test(&fltr, &document).unwrap());

    let fltr = Query::parse("tags: {name: vector AND value: giftwrap}").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["tags", "name"], vec!["tags", "value"]]);
    assert!(!AllFields::test(&fltr, &document).unwrap());

    let fltr = Query::parse("tags: {(name: (vector OR technique.*) AND value: giftwrap}").unwrap();
    assert_eq!(fltr.list_fields(), vec![vec!["tags", "name"], vec!["tags", "value"]]);
    assert!(AllFields::test(&fltr, &document).unwrap());
}
