use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

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
            let db = super::Database::new("test_e2e_db.txt");
            let db = std::sync::Arc::new(std::sync::Mutex::new(db));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
                .await
                .unwrap();
            println!("Test server listening on 127.0.0.1:8080");

            tx.send(()).unwrap(); // Signal that the server is ready

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

    std::fs::remove_file("test_e2e_db.txt").unwrap_or_default();
}

fn run_database_tests() {
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
