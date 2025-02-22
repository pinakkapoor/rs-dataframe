use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use crate::column::{Column, Value};
use crate::dataframe::DataFrame;

/// read a CSV file into a DataFrame.
///
/// auto-detects types: tries float first, falls back to string.
/// empty cells and "NA"/"NULL" become Value::Null.
pub fn read_csv(path: &Path) -> Result<DataFrame, String> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| format!("failed to open CSV: {e}"))?;

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| format!("failed to read headers: {e}"))?
        .iter()
        .map(|h| h.to_string())
        .collect();

    let ncols = headers.len();
    let mut col_data: Vec<Vec<Value>> = vec![Vec::new(); ncols];

    for result in reader.records() {
        let record = result.map_err(|e| format!("failed to read row: {e}"))?;
        for (i, field) in record.iter().enumerate() {
            if i < ncols {
                col_data[i].push(Value::parse(field));
            }
        }
    }

    let columns: Vec<Column> = headers
        .iter()
        .zip(col_data.into_iter())
        .map(|(name, values)| Column::new(name, values))
        .collect();

    DataFrame::new(columns)
}

/// write a DataFrame to CSV
pub fn write_csv(df: &DataFrame, path: &Path) -> Result<(), String> {
    let file = File::create(path).map_err(|e| format!("failed to create file: {e}"))?;
    let mut writer = BufWriter::new(file);

    // header
    let names: Vec<&str> = df.columns.iter().map(|c| c.name.as_str()).collect();
    writeln!(writer, "{}", names.join(",")).map_err(|e| format!("write error: {e}"))?;

    // rows
    for i in 0..df.nrows() {
        let row: Vec<String> = df.columns.iter().map(|c| {
            match &c.values[i] {
                Value::Float(f) => f.to_string(),
                Value::Text(s) => {
                    if s.contains(',') || s.contains('"') {
                        format!("\"{}\"", s.replace('"', "\"\""))
                    } else {
                        s.clone()
                    }
                }
                Value::Null => String::new(),
            }
        }).collect();
        writeln!(writer, "{}", row.join(",")).map_err(|e| format!("write error: {e}"))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as IoWrite;
    use tempfile::NamedTempFile;

    // skip CSV read/write tests if tempfile isn't available
    // (we don't have tempfile as a dep, these would need it)
}
