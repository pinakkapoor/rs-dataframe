/// A value in a cell — either a number, string, or null.
///
/// went back and forth on whether to use an enum or generics here.
/// enum is simpler and makes mixed-type columns possible (which
/// happens all the time with CSV data). generics would be more
/// efficient but way more complex for a learning project.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Float(f64),
    Text(String),
    Null,
}

impl Value {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// parse a string into the most specific type possible
    pub fn parse(s: &str) -> Value {
        if s.is_empty() || s == "NA" || s == "null" || s == "NULL" {
            Value::Null
        } else if let Ok(f) = s.parse::<f64>() {
            Value::Float(f)
        } else {
            Value::Text(s.to_string())
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Float(v) => write!(f, "{v}"),
            Value::Text(s) => write!(f, "{s}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

/// A named column of values.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub values: Vec<Value>,
}

impl Column {
    pub fn new(name: &str, values: Vec<Value>) -> Self {
        Column {
            name: name.to_string(),
            values,
        }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// count of non-null values
    pub fn count(&self) -> usize {
        self.values.iter().filter(|v| !v.is_null()).count()
    }

    /// sum of numeric values (ignores non-numeric)
    pub fn sum(&self) -> f64 {
        self.values
            .iter()
            .filter_map(|v| v.as_f64())
            .sum()
    }

    /// mean of numeric values
    pub fn mean(&self) -> Option<f64> {
        let nums: Vec<f64> = self.values.iter().filter_map(|v| v.as_f64()).collect();
        if nums.is_empty() {
            None
        } else {
            Some(nums.iter().sum::<f64>() / nums.len() as f64)
        }
    }

    /// min of numeric values
    pub fn min(&self) -> Option<f64> {
        self.values
            .iter()
            .filter_map(|v| v.as_f64())
            .fold(None, |min, v| Some(min.map_or(v, |m: f64| m.min(v))))
    }

    /// max of numeric values
    pub fn max(&self) -> Option<f64> {
        self.values
            .iter()
            .filter_map(|v| v.as_f64())
            .fold(None, |max, v| Some(max.map_or(v, |m: f64| m.max(v))))
    }

    /// standard deviation (sample)
    pub fn std(&self) -> Option<f64> {
        let mean = self.mean()?;
        let nums: Vec<f64> = self.values.iter().filter_map(|v| v.as_f64()).collect();
        if nums.len() < 2 {
            return None;
        }
        let variance: f64 = nums.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (nums.len() - 1) as f64;
        Some(variance.sqrt())
    }

    /// unique non-null values
    pub fn unique(&self) -> Vec<Value> {
        let mut seen = Vec::new();
        for v in &self.values {
            if !v.is_null() && !seen.contains(v) {
                seen.push(v.clone());
            }
        }
        seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_parse() {
        assert_eq!(Value::parse("42"), Value::Float(42.0));
        assert_eq!(Value::parse("hello"), Value::Text("hello".to_string()));
        assert_eq!(Value::parse(""), Value::Null);
        assert_eq!(Value::parse("NA"), Value::Null);
    }

    #[test]
    fn test_column_stats() {
        let col = Column::new("test", vec![
            Value::Float(1.0),
            Value::Float(2.0),
            Value::Float(3.0),
            Value::Null,
        ]);

        assert_eq!(col.len(), 4);
        assert_eq!(col.count(), 3);
        assert_eq!(col.sum(), 6.0);
        assert_eq!(col.mean(), Some(2.0));
        assert_eq!(col.min(), Some(1.0));
        assert_eq!(col.max(), Some(3.0));
    }
}
