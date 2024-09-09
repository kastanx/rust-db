use bincode::{deserialize_from, serialize_into};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Column {
    name: String,
    data_type: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Table {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Vec<String>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BTreeIndex {
    tree: BTreeMap<String, Vec<usize>>,
}

impl BTreeIndex {
    fn new() -> Self {
        BTreeIndex {
            tree: BTreeMap::new(),
        }
    }

    fn insert(&mut self, key: String, row_index: usize) {
        self.tree
            .entry(key)
            .or_insert_with(Vec::new)
            .push(row_index);
    }
}

#[derive(Serialize, Deserialize)]
struct DatabaseState {
    tables: HashMap<String, Table>,
    indexes: HashMap<String, BTreeIndex>,
}

struct Database {
    state: DatabaseState,
    file_path: String,
    dirty: bool,
    last_save: Instant,
    max_dirty_duration: Duration,
}

impl Database {
    fn new(file_path: &str) -> Self {
        let mut db = Database {
            state: DatabaseState {
                tables: HashMap::new(),
                indexes: HashMap::new(),
            },
            file_path: file_path.to_string(),
            dirty: false,
            last_save: Instant::now(),
            max_dirty_duration: Duration::from_secs(5),
        };
        db.load_from_file();
        db
    }

    fn load_from_file(&mut self) {
        if let Ok(file) = File::open(&self.file_path) {
            match deserialize_from(file) {
                Ok(state) => self.state = state,
                Err(e) => eprintln!("Error loading database: {}", e),
            }
        }
    }

    fn save_to_file(&self) {
        if let Ok(file) = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.file_path)
        {
            if let Err(e) = serialize_into(file, &self.state) {
                eprintln!("Error saving database: {}", e);
            }
        }
    }

    fn create_table(&mut self, table: Table) -> Result<(), String> {
        if self.state.tables.contains_key(&table.name) {
            return Err(format!("Table '{}' already exists", table.name));
        }
        let table_name = table.name.clone();
        self.state.tables.insert(table_name.clone(), table.clone());
        self.state.indexes.insert(table_name, BTreeIndex::new());
        self.save_to_file();
        Ok(())
    }

    fn insert(&mut self, table_name: &str, values: Vec<String>) -> Result<(), String> {
        if let Some(table) = self.state.tables.get_mut(table_name) {
            if values.len() != table.columns.len() {
                return Err("Number of values doesn't match number of columns".to_string());
            }
            let row_index = table.rows.len();
            table.rows.push(values.clone());

            if let Some(index) = self.state.indexes.get_mut(table_name) {
                for (i, value) in values.iter().enumerate() {
                    let key = format!("{}:{}", table.columns[i].name, value);
                    index.insert(key, row_index);
                }
            }

            self.dirty = true;
            self.save_if_needed();
            Ok(())
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    fn select(
        &self,
        table_name: &str,
        columns: Vec<String>,
        where_clause: Option<&str>,
    ) -> Result<Vec<Vec<String>>, String> {
        if let Some(table) = self.state.tables.get(table_name) {
            let column_indices: Vec<usize> = if columns.len() == 1 && columns[0] == "*" {
                (0..table.columns.len()).collect()
            } else {
                columns
                    .iter()
                    .map(|col| table.columns.iter().position(|c| c.name == *col))
                    .collect::<Option<Vec<usize>>>()
                    .ok_or_else(|| "One or more columns not found".to_string())?
            };

            let mut result = Vec::new();
            let header: Vec<String> = column_indices
                .iter()
                .map(|&i| table.columns[i].name.clone())
                .collect();
            result.push(header);

            let rows_to_process = if let Some(where_clause) = where_clause {
                self.apply_where_clause(table_name, where_clause)?
            } else {
                (0..table.rows.len()).collect()
            };

            for row_index in rows_to_process {
                let row = &table.rows[row_index];
                let selected_values: Vec<String> =
                    column_indices.iter().map(|&i| row[i].clone()).collect();
                result.push(selected_values);
            }

            Ok(result)
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    fn apply_where_clause(
        &self,
        table_name: &str,
        where_clause: &str,
    ) -> Result<Vec<usize>, String> {
        let parts: Vec<&str> = where_clause.split_whitespace().collect();
        if parts.len() != 3 {
            return Err("Invalid WHERE clause syntax".to_string());
        }

        let column = parts[0];
        let operator = parts[1];
        let value = parts[2];

        if let Some(table) = self.state.tables.get(table_name) {
            let column_index = table
                .columns
                .iter()
                .position(|c| c.name == column)
                .ok_or_else(|| format!("Column '{}' not found", column))?;

            match operator {
                "=" => {
                    let mut result = Vec::new();
                    for (i, row) in table.rows.iter().enumerate() {
                        if row[column_index] == value {
                            result.push(i);
                        }
                    }
                    Ok(result)
                }
                // Add support for other operators like >, <, >=, <= if needed
                _ => Err(format!("Unsupported operator: {}", operator)),
            }
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    fn save_if_needed(&mut self) {
        if self.dirty && self.last_save.elapsed() >= self.max_dirty_duration {
            self.save_to_file();
            self.dirty = false;
            self.last_save = Instant::now();
        }
    }
}

async fn handle_client(mut stream: tokio::net::TcpStream, db: Arc<Mutex<Database>>) {
    let mut buffer = [0; 1024];

    while let Ok(n) = stream.read(&mut buffer).await {
        if n == 0 {
            return;
        }

        let query = String::from_utf8_lossy(&buffer[..n]).to_string();
        let response = process_query(&query, &db);

        if let Err(e) = stream.write_all(response.as_bytes()).await {
            eprintln!("Failed to write to stream: {}", e);
            return;
        }
    }
}

fn process_query(query: &str, db: &Arc<Mutex<Database>>) -> String {
    let mut db = db.lock().unwrap();
    let parts: Vec<&str> = query.split_whitespace().collect();

    match parts[0].to_uppercase().as_str() {
        "CREATE" => {
            if parts.len() < 4 || parts[1].to_uppercase() != "TABLE" {
                return "Invalid CREATE TABLE syntax\n".to_string();
            }
            let table_name = parts[2];
            let columns: Vec<Column> = parts[3..]
                .chunks(2)
                .map(|chunk| Column {
                    name: chunk[0].to_string(),
                    data_type: chunk[1].to_string(),
                })
                .collect();
            let table = Table {
                name: table_name.to_string(),
                columns,
                rows: Vec::new(),
            };
            match db.create_table(table) {
                Ok(_) => "Table created successfully\n".to_string(),
                Err(e) => format!("Error creating table: {}\n", e),
            }
        }
        "INSERT" => {
            if parts.len() < 5
                || parts[1].to_uppercase() != "INTO"
                || parts[3].to_uppercase() != "VALUES"
            {
                return "Invalid INSERT syntax\n".to_string();
            }
            let table_name = parts[2];
            let values: Vec<String> = parts[4..].iter().map(|s| s.to_string()).collect();
            match db.insert(table_name, values) {
                Ok(_) => "Data inserted successfully\n".to_string(),
                Err(e) => format!("Error inserting data: {}\n", e),
            }
        }
        "SELECT" => {
            if parts.len() < 4 || !parts.contains(&"FROM") {
                return "Invalid SELECT syntax\n".to_string();
            }
            let from_position = parts
                .iter()
                .position(|&p| p.to_uppercase() == "FROM")
                .unwrap();
            let table_name = parts[from_position + 1];
            let where_position = parts.iter().position(|&p| p.to_uppercase() == "WHERE");

            let columns: Vec<String> = if parts[1] == "*" {
                vec!["*".to_string()]
            } else {
                parts[1..from_position]
                    .iter()
                    .filter(|&&c| c != ",")
                    .map(|&s| s.to_string())
                    .collect()
            };

            let where_clause = where_position.map(|pos| parts[pos + 1..].join(" "));

            match db.select(table_name, columns, where_clause.as_deref()) {
                Ok(results) => {
                    let mut response = String::new();
                    for row in results {
                        response.push_str(&row.join(" | "));
                        response.push('\n');
                    }
                    response
                }
                Err(e) => format!("Error executing query: {}\n", e),
            }
        }
        _ => "Invalid query\n".to_string(),
    }
}

#[cfg(test)]
mod tests;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Listening on 127.0.0.1:8080");

    let db = Arc::new(Mutex::new(Database::new("simple_db.bin")));

    loop {
        let (stream, _) = listener.accept().await?;
        let db_clone = Arc::clone(&db);

        tokio::spawn(async move {
            handle_client(stream, db_clone).await;
        });
    }
}

#[cfg(test)]
pub fn start_test_server(db_file: &str) -> std::io::Result<()> {
    use tokio::runtime::Runtime;

    let rt = Runtime::new()?;
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:8080").await?;
        println!("Test server listening on 127.0.0.1:8080");

        let db = Arc::new(Mutex::new(Database::new(db_file)));

        loop {
            let (stream, _) = listener.accept().await?;
            let db_clone = Arc::clone(&db);

            tokio::spawn(async move {
                handle_client(stream, db_clone).await;
            });
        }
    })
}
