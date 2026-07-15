use client::JsonRpcClient;
use common::JsonRpcRequest;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::sleep,
};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

async fn read_http_request(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 2048];
    loop {
        let read = stream.read(&mut chunk).await.unwrap();
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&chunk[..read]);
        if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8(bytes).unwrap()
}

async fn write_http_response(stream: &mut TcpStream, status: &str, body: &str) {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.unwrap();
}

#[tokio::test]
async fn mock_http_correlates_batch_and_checks_auth_header() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let request = read_http_request(&mut stream).await;
        assert!(request.to_ascii_lowercase().contains("x-test-key: integration"));
        let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
        let payload: Value = serde_json::from_str(body).unwrap();
        assert_eq!(payload.as_array().unwrap().len(), 2);
        write_http_response(
            &mut stream,
            "200 OK",
            &serde_json::to_string(&json!([
                {"jsonrpc":"2.0","id":2,"result":"second"},
                {"jsonrpc":"2.0","id":1,"result":"first"}
            ]))
            .unwrap(),
        )
        .await;
    });

    let mut headers = std::collections::BTreeMap::new();
    headers.insert("x-test-key".to_string(), "integration".to_string());
    let client = JsonRpcClient::new(format!("http://{address}"), Duration::from_secs(1))
        .unwrap()
        .with_headers(&headers)
        .unwrap();
    let requests = [
        JsonRpcRequest { jsonrpc: "2.0", id: 1, method: "first".into(), params: json!([]) },
        JsonRpcRequest { jsonrpc: "2.0", id: 2, method: "second".into(), params: json!([]) },
    ];
    let responses = client.call_batch(&requests).await.unwrap();
    assert_eq!(responses[0].id, Some(json!(1)));
    assert_eq!(responses[1].id, Some(json!(2)));
    server.await.unwrap();
}

#[tokio::test]
async fn mock_http_timeout_and_rate_limit_are_classified() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut stream).await;
        sleep(Duration::from_millis(100)).await;
    });
    let client =
        JsonRpcClient::new(format!("http://{address}"), Duration::from_millis(10)).unwrap();
    let error = client.call(1, "eth_blockNumber", json!([])).await.unwrap_err();
    assert!(error.to_string().to_ascii_lowercase().contains("timeout"));
    server.await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let _ = read_http_request(&mut stream).await;
        write_http_response(&mut stream, "429 Too Many Requests", "rate limited").await;
    });
    let client = JsonRpcClient::new(format!("http://{address}"), Duration::from_secs(1)).unwrap();
    let error = client.call(1, "eth_blockNumber", json!([])).await.unwrap_err();
    assert!(error.to_string().contains("HTTP 429"));
    server.await.unwrap();
}

#[tokio::test]
async fn mock_websocket_round_trip_is_deterministic() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let message = socket.next().await.unwrap().unwrap();
        let request: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        socket
            .send(Message::Text(
                json!({"jsonrpc":"2.0","id":request["id"],"result":"ok"}).to_string(),
            ))
            .await
            .unwrap();
    });

    let (mut socket, _) = connect_async(format!("ws://{address}")).await.unwrap();
    socket
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":7,"method":"eth_blockNumber","params":[]}).to_string(),
        ))
        .await
        .unwrap();
    let response = socket.next().await.unwrap().unwrap();
    let response: Value = serde_json::from_str(response.to_text().unwrap()).unwrap();
    assert_eq!(response["id"], 7);
    assert_eq!(response["result"], "ok");
    server.await.unwrap();
}
