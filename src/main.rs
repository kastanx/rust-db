use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Clone, Debug)]
struct Column {
    name: String,
    data_type: String,
}

#[derive(Clone, Debug)]
struct Table {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Vec<String>>,
}

struct Database {
    tables: HashMap<String, Table>,
    file_path: String,
}

impl Database {
    fn new(file_path: &str) -> Self {
        let mut db = Database {
            tables: HashMap::new(),
            file_path: file_path.to_string(),
        };
        db.load_from_file();
        db
    }

    fn load_from_file(&mut self) {
        if let Ok(file) = File::open(&self.file_path) {
            let reader = BufReader::new(file);
            let mut current_table: Option<Table> = None;

            for line in reader.lines() {
                if let Ok(line) = line {
                    if line.starts_with("TABLE:") {
                        if let Some(table) = current_table.take() {
                            self.tables.insert(table.name.clone(), table);
                        }
                        let parts: Vec<&str> = line.split(':').collect();
                        current_table = Some(Table {
                            name: parts[1].to_string(),
                            columns: Vec::new(),
                            rows: Vec::new(),
                        });
                    } else if line.starts_with("COLUMN:") {
                        let parts: Vec<&str> = line.split(':').collect();
                        if let Some(table) = current_table.as_mut() {
                            table.columns.push(Column {
                                name: parts[1].to_string(),
                                data_type: parts[2].to_string(),
                            });
                        }
                    } else if line.starts_with("ROW:") {
                        let parts: Vec<&str> = line.split(':').collect();
                        let values: Vec<String> = parts[1].split(',').map(String::from).collect();
                        if let Some(table) = current_table.as_mut() {
                            table.rows.push(values);
                        }
                    }
                }
            }

            if let Some(table) = current_table.take() {
                self.tables.insert(table.name.clone(), table);
            }
        }
    }

    fn save_to_file(&self) {
        if let Ok(mut file) = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&self.file_path)
        {
            for (_, table) in &self.tables {
                writeln!(file, "TABLE:{}", table.name).unwrap();
                for column in &table.columns {
                    writeln!(file, "COLUMN:{}:{}", column.name, column.data_type).unwrap();
                }
                for row in &table.rows {
                    writeln!(file, "ROW:{}", row.join(",")).unwrap();
                }
            }
        }
    }

    fn create_table(&mut self, table: Table) -> Result<(), String> {
        if self.tables.contains_key(&table.name) {
            return Err(format!("Table '{}' already exists", table.name));
        }
        self.tables.insert(table.name.clone(), table);
        self.save_to_file();
        Ok(())
    }

    fn insert(&mut self, table_name: &str, values: Vec<String>) -> Result<(), String> {
        if let Some(table) = self.tables.get_mut(table_name) {
            if values.len() != table.columns.len() {
                return Err("Number of values doesn't match number of columns".to_string());
            }
            table.rows.push(values);
            self.save_to_file();
            Ok(())
        } else {
            Err(format!("Table '{}' not found", table_name))
        }
    }

    fn select(&self, table_name: &str, columns: Vec<String>) -> Result<Vec<Vec<String>>, String> {
        if let Some(table) = self.tables.get(table_name) {
            let column_indices: Vec<usize> = columns
                .iter()
                .map(|col| table.columns.iter().position(|c| c.name == *col))
                .collect::<Option<Vec<usize>>>()
                .ok_or_else(|| "One or more columns not found".to_string())?;

            let mut result = Vec::new();
            result.push(columns.clone());

            for row in &table.rows {
                let selected_values: Vec<String> =
                    column_indices.iter().map(|&i| row[i].clone()).collect();
                result.push(selected_values);
            }

            Ok(result)
        } else {
            Err(format!("Table '{}' not found", table_name))
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
            if parts.len() < 4 || parts[parts.len() - 2].to_uppercase() != "FROM" {
                return "Invalid SELECT syntax\n".to_string();
            }
            let table_name = parts[parts.len() - 1];
            let columns: Vec<String> = parts[1..parts.len() - 2]
                .iter()
                .map(|s| s.to_string())
                .collect();
            match db.select(table_name, columns) {
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Listening on 127.0.0.1:8080");

    let db = Arc::new(Mutex::new(Database::new("simple_db.txt")));

    loop {
        let (stream, _) = listener.accept().await?;
        let db_clone = Arc::clone(&db);

        tokio::spawn(async move {
            handle_client(stream, db_clone).await;
        });
    }
}
