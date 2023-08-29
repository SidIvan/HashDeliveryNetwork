use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};
use serde_json::{Result, Value};
use mongodb::{bson::doc, options::{ClientOptions, ServerApi, ServerApiVersion}, Client, Collection};

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

async fn store_action(input_data: StoreInputData) -> Response {
    let mut opts = ClientOptions::parse("mongodb://localhost:27017").await.unwrap();
    opts.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
    let collection: Collection<Document> = Client::with_options(opts).unwrap().database("hash_db").collection("hashes");    
    collection.insert_one(Document{key: input_data.key, hash: input_data.hash}, None).await;
    Response::SuccessStore
}

async fn load_action(input_data: StoreOutputData) -> Response {
    let mut opts = ClientOptions::parse("mongodb://localhost:27017").await.unwrap();
    opts.server_api = Some(ServerApi::builder().version(ServerApiVersion::V1).build());
    let collection: Collection<Document> = Client::with_options(opts).unwrap().database("hash_db").collection("hashes");    
    let res = collection.find_one(Some(doc!{"key": input_data.key}), None).await.unwrap();
    match res {
        Some(doc) => Response::SuccessLoad(doc),
        None => Response::FailedLoad,
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8181").await.unwrap();
    loop {
        let (mut socket, _) = listener.accept().await.unwrap();
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
                        match input_data["request_type"].as_str().unwrap() {
                            "store" => {
                                let res = tokio::spawn(store_action(serde_json::from_slice::<StoreInputData>(&buf[0..num_bytes]).unwrap())).await;
                                let mut b: Vec<u8> = Vec::new();
                                let out = StoreResponse{response_status: "success".to_string()};
                                serde_json::to_writer(&mut b, &out);
                                socket.write_all(&b).await;
                            }, 
                           "load" => {
                                let res = tokio::spawn(load_action(serde_json::from_slice::<StoreOutputData>(&buf[0..num_bytes]).unwrap())).await;
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
