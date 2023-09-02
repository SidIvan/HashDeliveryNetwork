use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Deserialize)]
struct StoreInputData {
    request_type: String, 
    key: String,
    hash: String,
}

#[derive(Deserialize)]
struct StoreOutputData {
    request_type: String, 
    key: String,
}

#[derive(Serialize)]
struct StoreResponse {
    response_status: String,
}

#[derive(Serialize)]
struct LoadSuccessResponse {
    response_status: String,
    requested_key: String,
    requested_hash: String,
}

#[derive(Serialize)]
struct LoadFailedResponse {
    response_status: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Document {
    key: String,
    hash: String,
}

enum Response {
    SuccessStore,
    SuccessLoad(Document),
    FailedLoad
}

async fn store_action(input_data: StoreInputData, hash_map_mutex: Arc<Mutex<HashMap<String, String>>>) -> Response {
    let mut hash_map = hash_map_mutex.lock().await;
    hash_map.insert(input_data.key, input_data.hash);
    Response::SuccessStore
}

async fn load_action(input_data: StoreOutputData, hash_map_mutex: Arc<Mutex<HashMap<String, String>>>) -> Response {
    let hash_map = hash_map_mutex.lock().await;
    match hash_map.get(&input_data.key) {
        Some(doc) => Response::SuccessLoad(Document{key: input_data.key, hash: doc.to_string()}),
        None => Response::FailedLoad,
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8181").await.unwrap();
    let mutex = Arc::new(Mutex::new(HashMap::new()));
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mutex = Arc::clone(&mutex);
        tokio::spawn(async move {
            let mut buf = [0; 1024];
            loop {
                let num_bytes = match socket.read(&mut buf).await {
                    Ok(num_bytes) if num_bytes == 0 => return,
                    Ok(num_bytes) => num_bytes,
                    Err(err) => {
                        eprintln!("read from socket error; {:?}", err);
                        return;
                    }
                };
                match serde_json::from_slice::<Value>(&buf[0..num_bytes]) {
                    Ok(input_data) => {
                        let mutex = Arc::clone(&mutex);
                        match input_data["request_type"].as_str().unwrap() {
                            "store" => {
                                tokio::spawn(store_action(serde_json::from_slice::<StoreInputData>(&buf[0..num_bytes]).unwrap(), mutex)).await;
                                let mut b: Vec<u8> = Vec::new();
                                let out = StoreResponse{response_status: "success".to_string()};
                                serde_json::to_writer(&mut b, &out);
                                socket.write_all(&b).await;
                            }, 
                           "load" => {
                                let res = tokio::spawn(load_action(serde_json::from_slice::<StoreOutputData>(&buf[0..num_bytes]).unwrap(), mutex)).await;
                                match res {
                                    Ok(Response::SuccessLoad(r)) => {
                                        let mut b: Vec<u8> = Vec::new();
                                        let out = LoadSuccessResponse{response_status: "success".to_string(), requested_key: r.key, requested_hash: r.hash};
                                        serde_json::to_writer(&mut b, &out);
                                        socket.write_all(&b).await;
                                    }
                                    Ok(Response::FailedLoad) => {
                                        let mut b: Vec<u8> = Vec::new();
                                        let out = LoadFailedResponse{response_status: "failed".to_string()};
                                        serde_json::to_writer(&mut b, &out);
                                        socket.write_all(&b).await;
                                    }
                                    Err(err) => {
                                        eprintln!("{:?}", err);
                                    }
                                    Ok(_) => return
                                }
                            },
                            _ => eprintln!("invalid request type")
                        }
                    },
                    Err(err) => eprintln!("invalid json format; {:?}", err)
                }
            }
        });
    }
}
