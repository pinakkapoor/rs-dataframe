use std::collections::HashMap;
use crate::column::{Column, Value};

/// A DataFrame is a collection of named columns with equal length.
///
/// the borrow checker made this way harder than i expected. my first
/// version tried to store references to columns but lifetimes got
/// unmanageable fast. cloning is less efficient but actually works.
#[derive(Debug, Clone)]
pub struct DataFrame {
    pub columns: Vec<Column>,
}

impl DataFrame {
    pub fn new(columns: Vec<Column>) -> Result<Self, String> {
        if columns.is_empty() {
            return Ok(DataFrame { columns });
        }

        let expected_len = columns[0].len();
        for col in &columns {
            if col.len() != expected_len {
                return Err(format!(
                    "column '{}' has {} rows, expected {}",
                    col.name,
                    col.len(),
                    expected_len
                ));
            }
        }

        Ok(DataFrame { columns })
    }

    pub fn nrows(&self) -> usize {
        self.columns.first().map_or(0, |c| c.len())
    }

    pub fn ncols(&self) -> usize {
        self.columns.len()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// get a column by name
    pub fn col(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// select specific columns by name
    pub fn select(&self, names: &[&str]) -> Result<DataFrame, String> {
        let cols: Result<Vec<Column>, String> = names
            .iter()
            .map(|name| {
                self.col(name)
                    .map(|c| c.clone())
                    .ok_or_else(|| format!("column '{}' not found", name))
            })
            .collect();

        DataFrame::new(cols?)
    }

    /// filter rows where the predicate returns true for the given column
    pub fn filter(&self, col_name: &str, predicate: impl Fn(&Value) -> bool) -> Result<DataFrame, String> {
        let col = self.col(col_name).ok_or_else(|| format!("column '{}' not found", col_name))?;

        let mask: Vec<bool> = col.values.iter().map(&predicate).collect();

        let new_cols = self
            .columns
            .iter()
            .map(|c| {
                let filtered: Vec<Value> = c
                    .values
                    .iter()
                    .zip(mask.iter())
                    .filter(|(_, &keep)| keep)
                    .map(|(v, _)| v.clone())
                    .collect();
                Column::new(&c.name, filtered)
            })
            .collect();

        DataFrame::new(new_cols)
    }

    /// sort by a column (ascending). only works for numeric columns.
    pub fn sort_by(&self, col_name: &str, ascending: bool) -> Result<DataFrame, String> {
        let col = self.col(col_name).ok_or_else(|| format!("column '{}' not found", col_name))?;

        let mut indices: Vec<usize> = (0..self.nrows()).collect();
        indices.sort_by(|&a, &b| {
            let va = col.values[a].as_f64().unwrap_or(f64::NAN);
            let vb = col.values[b].as_f64().unwrap_or(f64::NAN);
            if ascending {
                va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
            } else {
                vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
            }
        });

        let new_cols = self
            .columns
            .iter()
            .map(|c| {
                let sorted: Vec<Value> = indices.iter().map(|&i| c.values[i].clone()).collect();
                Column::new(&c.name, sorted)
            })
            .collect();

        DataFrame::new(new_cols)
    }

    /// group by a column and aggregate another with a function.
    ///
    /// this is where ownership got really tricky. collecting into
    /// HashMaps of Vec<f64> was the cleanest approach i found.
    pub fn groupby_agg(
        &self,
        group_col: &str,
        agg_col: &str,
        agg_fn: impl Fn(&[f64]) -> f64,
    ) -> Result<DataFrame, String> {
        let gcol = self.col(group_col).ok_or_else(|| format!("column '{}' not found", group_col))?;
        let acol = self.col(agg_col).ok_or_else(|| format!("column '{}' not found", agg_col))?;

        let mut groups: HashMap<String, Vec<f64>> = HashMap::new();

        for i in 0..self.nrows() {
            let key = gcol.values[i].to_string();
            let val = acol.values[i].as_f64().unwrap_or(f64::NAN);
            groups.entry(key).or_default().push(val);
        }

        let mut keys: Vec<String> = groups.keys().cloned().collect();
        keys.sort(); // deterministic order

        let group_values: Vec<Value> = keys.iter().map(|k| Value::Text(k.clone())).collect();
        let agg_values: Vec<Value> = keys
            .iter()
            .map(|k| Value::Float(agg_fn(&groups[k])))
            .collect();

        DataFrame::new(vec![
            Column::new(group_col, group_values),
            Column::new(&format!("{agg_col}_agg"), agg_values),
        ])
    }

    /// inner join on a common column
    pub fn join(&self, other: &DataFrame, on: &str) -> Result<DataFrame, String> {
        let self_col = self.col(on).ok_or_else(|| format!("column '{}' not found in left", on))?;
        let other_col = other.col(on).ok_or_else(|| format!("column '{}' not found in right", on))?;

        // build index on right side
        let mut right_index: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, v) in other_col.values.iter().enumerate() {
            right_index.entry(v.to_string()).or_default().push(i);
        }

        // for each left row, find matching right rows
        let mut result_cols: Vec<Vec<Value>> = vec![Vec::new(); self.ncols() + other.ncols() - 1];
        let other_col_indices: Vec<usize> = (0..other.ncols())
            .filter(|&i| other.columns[i].name != on)
            .collect();

        for left_row in 0..self.nrows() {
            let key = self_col.values[left_row].to_string();
            if let Some(right_rows) = right_index.get(&key) {
                for &right_row in right_rows {
                    // add left columns
                    for (col_idx, col) in self.columns.iter().enumerate() {
                        result_cols[col_idx].push(col.values[left_row].clone());
                    }
                    // add right columns (except join key)
                    for (offset, &rcol_idx) in other_col_indices.iter().enumerate() {
                        result_cols[self.ncols() + offset]
                            .push(other.columns[rcol_idx].values[right_row].clone());
                    }
                }
            }
        }

        let mut new_columns = Vec::new();
        for (i, col) in self.columns.iter().enumerate() {
            new_columns.push(Column::new(&col.name, result_cols[i].clone()));
        }
        for (offset, &rcol_idx) in other_col_indices.iter().enumerate() {
            new_columns.push(Column::new(
                &other.columns[rcol_idx].name,
                result_cols[self.ncols() + offset].clone(),
            ));
        }

        DataFrame::new(new_columns)
    }

    /// head — first n rows
    pub fn head(&self, n: usize) -> DataFrame {
        let n = n.min(self.nrows());
        let cols = self
            .columns
            .iter()
            .map(|c| Column::new(&c.name, c.values[..n].to_vec()))
            .collect();
        DataFrame { columns: cols }
    }

    /// describe — summary statistics for numeric columns
    pub fn describe(&self) -> String {
        let mut output = String::new();
        for col in &self.columns {
            if col.mean().is_some() {
                output.push_str(&format!(
                    "{}: count={}, mean={:.2}, min={:.2}, max={:.2}, std={:.2}\n",
                    col.name,
                    col.count(),
                    col.mean().unwrap_or(0.0),
                    col.min().unwrap_or(0.0),
                    col.max().unwrap_or(0.0),
                    col.std().unwrap_or(0.0),
                ));
            }
        }
        output
    }

    /// pretty-print the first n rows
    pub fn display(&self, n: usize) -> String {
        let n = n.min(self.nrows());
        let mut output = String::new();

        // header
        let names: Vec<String> = self.columns.iter().map(|c| c.name.clone()).collect();
        output.push_str(&names.join("\t"));
        output.push('\n');

        // rows
        for i in 0..n {
            let row: Vec<String> = self.columns.iter().map(|c| c.values[i].to_string()).collect();
            output.push_str(&row.join("\t"));
            output.push('\n');
        }

        output.push_str(&format!("[{} rows x {} columns]\n", self.nrows(), self.ncols()));
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_df() -> DataFrame {
        DataFrame::new(vec![
            Column::new("name", vec![
                Value::Text("alice".into()),
                Value::Text("bob".into()),
                Value::Text("charlie".into()),
            ]),
            Column::new("age", vec![
                Value::Float(30.0),
                Value::Float(25.0),
                Value::Float(35.0),
            ]),
            Column::new("dept", vec![
                Value::Text("eng".into()),
                Value::Text("eng".into()),
                Value::Text("sales".into()),
            ]),
        ])
        .unwrap()
    }

    #[test]
    fn test_basic_properties() {
        let df = sample_df();
        assert_eq!(df.nrows(), 3);
        assert_eq!(df.ncols(), 3);
        assert_eq!(df.column_names(), vec!["name", "age", "dept"]);
    }

    #[test]
    fn test_select() {
        let df = sample_df();
        let selected = df.select(&["name", "age"]).unwrap();
        assert_eq!(selected.ncols(), 2);
        assert_eq!(selected.column_names(), vec!["name", "age"]);
    }

    #[test]
    fn test_filter() {
        let df = sample_df();
        let filtered = df
            .filter("age", |v| v.as_f64().map_or(false, |a| a > 28.0))
            .unwrap();
        assert_eq!(filtered.nrows(), 2);
    }

    #[test]
    fn test_sort() {
        let df = sample_df();
        let sorted = df.sort_by("age", true).unwrap();
        let ages = sorted.col("age").unwrap();
        assert_eq!(ages.values[0], Value::Float(25.0));
        assert_eq!(ages.values[2], Value::Float(35.0));
    }

    #[test]
    fn test_groupby() {
        let df = sample_df();
        let grouped = df
            .groupby_agg("dept", "age", |vals| vals.iter().sum::<f64>() / vals.len() as f64)
            .unwrap();
        assert_eq!(grouped.nrows(), 2); // eng and sales
    }

    #[test]
    fn test_join() {
        let left = DataFrame::new(vec![
            Column::new("id", vec![Value::Float(1.0), Value::Float(2.0)]),
            Column::new("name", vec![Value::Text("a".into()), Value::Text("b".into())]),
        ]).unwrap();

        let right = DataFrame::new(vec![
            Column::new("id", vec![Value::Float(1.0), Value::Float(2.0)]),
            Column::new("score", vec![Value::Float(90.0), Value::Float(80.0)]),
        ]).unwrap();

        let joined = left.join(&right, "id").unwrap();
        assert_eq!(joined.ncols(), 3); // id, name, score
        assert_eq!(joined.nrows(), 2);
    }
}
