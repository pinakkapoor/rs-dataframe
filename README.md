# rs-dataframe

learning rust by building a tiny dataframe library. it's not polars, but building it taught me more about ownership and borrowing than any tutorial.

## what it does

- read/write CSV files with auto type detection
- column operations: filter, sort, select
- groupby with custom aggregation functions
- inner join on a common column
- basic statistics: mean, min, max, std, count
- describe() and display() for quick inspection

## example

```rust
use rs_dataframe::{DataFrame, Column, Value};

let df = DataFrame::new(vec![
    Column::new("name", vec![
        Value::Text("alice".into()),
        Value::Text("bob".into()),
    ]),
    Column::new("score", vec![
        Value::Float(95.0),
        Value::Float(87.0),
    ]),
]).unwrap();

// filter
let high_scores = df.filter("score", |v| {
    v.as_f64().map_or(false, |s| s > 90.0)
}).unwrap();

// groupby
let by_dept = df.groupby_agg("dept", "score", |vals| {
    vals.iter().sum::<f64>() / vals.len() as f64
}).unwrap();
```

## what i learned

- the borrow checker forces you to think about data ownership upfront. my first version tried to use references everywhere and the lifetime annotations became unmanageable. cloning is fine for a learning project
- rust enums are perfect for sum types like `Value` — pattern matching catches missing cases at compile time
- `HashMap<String, Vec<f64>>` for groupby feels inelegant but works. tried a more generic approach with trait objects and it was 10x more complex
- integer indexing with `Vec<usize>` for sort/filter is the same pattern as numpy fancy indexing, just explicit

## limitations

this is a learning project, not a real library. for actual work use polars or arrow-rs.

- no lazy evaluation — everything is eager
- lots of cloning (a real impl would use Arc or copy-on-write)
- no parallel execution
- CSV only (no parquet, json, etc)
