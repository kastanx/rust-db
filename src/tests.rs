use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use tokio::time::Instant;

fn send_query(query: &str) -> String {
    let mut stream = TcpStream::connect("127.0.0.1:8080").unwrap();
    stream.write_all(query.as_bytes()).unwrap();
    stream.shutdown(std::net::Shutdown::Write).unwrap();

    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
}

#[test]
fn test_e2e_database_operations() {
    let (tx, rx) = mpsc::channel();

    let server_thread = thread::spawn(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let db = super::Database::new("test_e2e_db.bin");
            let db = std::sync::Arc::new(std::sync::Mutex::new(db));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
                .await
                .unwrap();
            println!("Test server listening on 127.0.0.1:8080");

            tx.send(()).unwrap();

            loop {
                tokio::select! {
                    Ok((stream, _)) = listener.accept() => {
                        let db_clone = std::sync::Arc::clone(&db);
                        tokio::spawn(async move {
                            super::handle_client(stream, db_clone).await;
                        });
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(5)) => {
                        println!("Test server shutting down");
                        break;
                    }
                }
            }
        });
    });

    rx.recv_timeout(Duration::from_secs(5)).unwrap();

    run_database_tests();

    server_thread.join().unwrap();

    std::fs::remove_file("test_e2e_db.bin").unwrap_or_default();
}

fn run_database_tests() {
    test_basic_operations();
    test_multiple_tables();
    test_performance();
}

fn test_basic_operations() {
    let create_response = send_query("CREATE TABLE users id STRING name STRING age INTEGER");
    assert_eq!(create_response, "Table created successfully\n");

    let insert_response = send_query("INSERT INTO users VALUES 1 John 30");
    assert_eq!(insert_response, "Data inserted successfully\n");

    let select_all_response = send_query("SELECT * FROM users");
    assert!(select_all_response.contains("id | name | age"));
    assert!(select_all_response.contains("1 | John | 30"));

    send_query("INSERT INTO users VALUES 2 Jane 25");

    let select_where_response = send_query("SELECT * FROM users WHERE age = 30");
    assert!(select_where_response.contains("id | name | age"));
    assert!(select_where_response.contains("1 | John | 30"));
    assert!(!select_where_response.contains("2 | Jane | 25"));

    let invalid_create = send_query("CREATE TABLE users id STRING");
    assert!(invalid_create.contains("Error creating table"));

    let invalid_insert = send_query("INSERT INTO users VALUES 3 Bob");
    assert!(invalid_insert.contains("Error inserting data"));

    let invalid_select = send_query("SELECT * FROM nonexistent_table");
    assert!(invalid_select.contains("Error executing query"));
}

fn test_multiple_tables() {
    send_query("CREATE TABLE products id INTEGER name STRING price FLOAT");
    send_query("CREATE TABLE orders id INTEGER product_id INTEGER quantity INTEGER");

    send_query("INSERT INTO products VALUES 1 Apple 0.5");
    send_query("INSERT INTO products VALUES 2 Banana 0.3");
    send_query("INSERT INTO orders VALUES 1 1 10");
    send_query("INSERT INTO orders VALUES 2 2 15");

    let products_response = send_query("SELECT * FROM products");
    assert!(products_response.contains("1 | Apple | 0.5"));
    assert!(products_response.contains("2 | Banana | 0.3"));

    let orders_response = send_query("SELECT * FROM orders");
    assert!(orders_response.contains("1 | 1 | 10"));
    assert!(orders_response.contains("2 | 2 | 15"));

    let product_where_response = send_query("SELECT name FROM products WHERE price = 0.5");
    assert!(product_where_response.contains("Apple"));
    assert!(!product_where_response.contains("Banana"));
}

fn test_performance() {
    send_query("CREATE TABLE large_data id INTEGER value STRING");

    let start_time = Instant::now();
    for i in 0..10000 {
        send_query(&format!("INSERT INTO large_data VALUES {} value_{}", i, i));
    }
    let insert_duration = start_time.elapsed();
    println!("Time to insert 10000 rows: {:?}", insert_duration);

    let start_time = Instant::now();
    let _large_select_response = send_query("SELECT * FROM large_data");
    let select_all_duration = start_time.elapsed();
    println!("Time to select 10000 rows: {:?}", select_all_duration);

    let start_time = Instant::now();
    let _where_response = send_query("SELECT * FROM large_data WHERE id = 5000");
    let select_where_duration = start_time.elapsed();
    println!(
        "Time to select one row with WHERE clause: {:?}",
        select_where_duration
    );

    assert!(
        insert_duration < Duration::from_secs(3),
        "Insert operation took too long"
    );
    assert!(
        select_all_duration < Duration::from_secs(10),
        "Select all operation took too long"
    );
    assert!(
        select_where_duration < Duration::from_secs(1),
        "Select with WHERE clause took too long"
    );
}
