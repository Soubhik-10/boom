use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use common::{JsonRpcRequest, JsonRpcResponse};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_LENGTH};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

type HmacSha256 = Hmac<Sha256>;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
pub struct JsonRpcClient {
    url: String,
    http: reqwest::Client,
    jwt: Option<JwtSigner>,
    headers: HeaderMap,
}

#[derive(Clone)]
pub struct JwtSigner {
    secret: Vec<u8>,
}

impl JsonRpcClient {
    pub fn new(url: impl Into<String>, timeout: Duration) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .build()?;
        Ok(Self { url: url.into(), http, jwt: None, headers: HeaderMap::new() })
    }

    pub fn with_jwt(mut self, signer: JwtSigner) -> Self {
        self.jwt = Some(signer);
        self
    }

    pub fn with_headers(mut self, headers: &BTreeMap<String, String>) -> Result<Self> {
        for (name, value) in headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .with_context(|| format!("invalid HTTP header name '{name}'"))?;
            let value = HeaderValue::from_str(value)
                .with_context(|| format!("invalid value for HTTP header '{name}'"))?;
            self.headers.insert(name, value);
        }
        Ok(self)
    }

    pub async fn call(
        &self,
        id: u64,
        method: impl Into<String>,
        params: Value,
    ) -> Result<JsonRpcResponse> {
        let request = JsonRpcRequest { jsonrpc: "2.0", id, method: method.into(), params };
        let response = self.send_json(&request).await?;
        decode_response(response, id)
    }

    pub async fn call_batch(&self, requests: &[JsonRpcRequest]) -> Result<Vec<JsonRpcResponse>> {
        anyhow::ensure!(!requests.is_empty(), "JSON-RPC batch cannot be empty");
        let value = self.send_json(requests).await?;
        let values =
            value.as_array().ok_or_else(|| anyhow!("JSON-RPC batch response must be an array"))?;
        order_batch_responses(requests, values)
    }

    async fn send_json<T: serde::Serialize + ?Sized>(&self, payload: &T) -> Result<Value> {
        let mut headers = self.headers.clone();
        if let Some(jwt) = &self.jwt {
            let value = format!("Bearer {}", jwt.token()?);
            headers.insert(AUTHORIZATION, HeaderValue::from_str(&value)?);
        }

        let response = self
            .http
            .post(&self.url)
            .headers(headers)
            .json(payload)
            .send()
            .await
            .map_err(classify_reqwest_error)?;
        let status = response.status();
        if let Some(length) = response
            .headers()
            .get(CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
        {
            anyhow::ensure!(
                length <= MAX_RESPONSE_BYTES,
                "response body exceeds {MAX_RESPONSE_BYTES} bytes"
            );
        }
        let body = response.bytes().await.map_err(classify_reqwest_error)?;
        anyhow::ensure!(
            body.len() <= MAX_RESPONSE_BYTES,
            "response body exceeds {MAX_RESPONSE_BYTES} bytes"
        );
        if !status.is_success() {
            let mut body = String::from_utf8_lossy(&body).into_owned();
            body.truncate(4_096);
            return Err(anyhow!("HTTP {status}: {body}"));
        }
        serde_json::from_slice(&body).with_context(|| {
            let preview = String::from_utf8_lossy(&body[..body.len().min(4_096)]);
            format!("decoding JSON-RPC response body: {preview}")
        })
    }
}

fn order_batch_responses(
    requests: &[JsonRpcRequest],
    values: &[Value],
) -> Result<Vec<JsonRpcResponse>> {
    let expected = requests
        .iter()
        .enumerate()
        .map(|(index, request)| (request.id, index))
        .collect::<HashMap<_, _>>();
    anyhow::ensure!(
        expected.len() == requests.len(),
        "JSON-RPC batch contains duplicate request IDs"
    );
    let mut ordered = vec![None; requests.len()];
    for value in values {
        let id = response_id(value)?;
        let index = expected
            .get(&id)
            .copied()
            .ok_or_else(|| anyhow!("unexpected JSON-RPC batch response ID {id}"))?;
        anyhow::ensure!(ordered[index].is_none(), "duplicate JSON-RPC batch response ID {id}");
        ordered[index] = Some(decode_response(value.clone(), id)?);
    }
    requests
        .iter()
        .zip(ordered)
        .map(|(request, response)| {
            response.ok_or_else(|| anyhow!("missing JSON-RPC batch response ID {}", request.id))
        })
        .collect()
}

fn classify_reqwest_error(error: reqwest::Error) -> anyhow::Error {
    if error.is_timeout() {
        anyhow!("timeout: {error}")
    } else {
        error.into()
    }
}

fn response_id(value: &Value) -> Result<u64> {
    value
        .get("id")
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("JSON-RPC response is missing a numeric ID"))
}

fn decode_response(value: Value, expected_id: u64) -> Result<JsonRpcResponse> {
    let object = value.as_object().ok_or_else(|| anyhow!("JSON-RPC response must be an object"))?;
    anyhow::ensure!(
        object.get("jsonrpc").and_then(Value::as_str) == Some("2.0"),
        "JSON-RPC response has an invalid or missing version"
    );
    let id = response_id(&value)?;
    anyhow::ensure!(
        id == expected_id,
        "JSON-RPC response ID {id} does not match request ID {expected_id}"
    );
    let has_result = object.contains_key("result");
    let has_error = object.get("error").is_some_and(|error| !error.is_null());
    anyhow::ensure!(
        has_result ^ has_error,
        "JSON-RPC response must contain exactly one of result or error"
    );
    serde_json::from_value(value).context("decoding JSON-RPC response")
}

impl JwtSigner {
    pub fn from_file_or_hex(input: &str) -> Result<Self> {
        let raw = if Path::new(input).exists() {
            fs::read_to_string(input).with_context(|| format!("reading JWT secret file {input}"))?
        } else {
            input.to_string()
        };
        let normalized = raw.trim().trim_start_matches("0x");
        Ok(Self { secret: decode_hex(normalized)? })
    }

    pub fn token(&self) -> Result<String> {
        let iat = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        let header = json!({"alg":"HS256","typ":"JWT"});
        let claims = json!({"iat": iat});
        let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?);
        let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims)?);
        let signing_input = format!("{header}.{claims}");
        let mut mac = HmacSha256::new_from_slice(&self.secret)?;
        mac.update(signing_input.as_bytes());
        let signature = URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes());
        Ok(format!("{signing_input}.{signature}"))
    }
}

fn decode_hex(input: &str) -> Result<Vec<u8>> {
    if !input.len().is_multiple_of(2) {
        return Err(anyhow!("JWT secret hex has odd length"));
    }
    let mut out = Vec::with_capacity(input.len() / 2);
    let bytes = input.as_bytes();
    let (chunks, remainder) = bytes.as_chunks::<2>();
    debug_assert!(remainder.is_empty());
    for [hi, lo] in chunks {
        let hi = from_hex(*hi)?;
        let lo = from_hex(*lo)?;
        out.push((hi << 4) | lo);
    }
    Ok(out)
}

fn from_hex(byte: u8) -> Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(anyhow!("invalid hex character in JWT secret")),
    }
}

#[derive(Clone)]
pub struct RestClient {
    base_url: String,
    http: reqwest::Client,
    jwt: Option<JwtSigner>,
}

#[derive(Debug, Clone)]
pub struct RestResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub bytes: Vec<u8>,
}

impl RestClient {
    pub fn new(base_url: impl Into<String>, timeout: Duration) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(60))
            .build()?;
        Ok(Self { base_url: base_url.into().trim_end_matches('/').to_string(), http, jwt: None })
    }

    pub fn with_jwt(mut self, signer: JwtSigner) -> Self {
        self.jwt = Some(signer);
        self
    }

    pub async fn get(&self, path: &str, accept: Option<&str>) -> Result<RestResponse> {
        let url = self.url(path);
        let mut request = self.http.get(url);
        if let Some(accept) = accept {
            request = request.header(reqwest::header::ACCEPT, accept);
        }
        self.send(request).await
    }

    pub async fn post(
        &self,
        path: &str,
        content_type: Option<&str>,
        accept: Option<&str>,
        body: Vec<u8>,
    ) -> Result<RestResponse> {
        let url = self.url(path);
        let mut request = self.http.post(url).body(body);
        if let Some(content_type) = content_type {
            request = request.header(reqwest::header::CONTENT_TYPE, content_type);
        }
        if let Some(accept) = accept {
            request = request.header(reqwest::header::ACCEPT, accept);
        }
        self.send(request).await
    }

    async fn send(&self, mut request: reqwest::RequestBuilder) -> Result<RestResponse> {
        if let Some(jwt) = &self.jwt {
            request = request.bearer_auth(jwt.token()?);
        }
        let response = request.send().await?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        let bytes = response.bytes().await?.to_vec();
        Ok(RestResponse { status, content_type, bytes })
    }

    fn url(&self, path: &str) -> String {
        if path.starts_with('/') {
            format!("{}{}", self.base_url, path)
        } else {
            format!("{}/{}", self.base_url, path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use serde_json::Value;

    #[test]
    fn jwt_token_has_header_claims_and_signature() {
        let signer = JwtSigner::from_file_or_hex("000102030405060708090a0b0c0d0e0f").unwrap();
        let token = signer.token().unwrap();
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let header = URL_SAFE_NO_PAD.decode(parts[0]).unwrap();
        let header: Value = serde_json::from_slice(&header).unwrap();
        assert_eq!(header["alg"], "HS256");
        assert_eq!(header["typ"], "JWT");

        let claims = URL_SAFE_NO_PAD.decode(parts[1]).unwrap();
        let claims: Value = serde_json::from_slice(&claims).unwrap();
        assert!(claims["iat"].as_u64().is_some());
        assert!(!parts[2].is_empty());
    }

    #[test]
    fn rejects_bad_hex_secret() {
        assert!(JwtSigner::from_file_or_hex("abc").is_err());
        assert!(JwtSigner::from_file_or_hex("zz").is_err());
    }

    #[test]
    fn rest_client_joins_paths_predictably() {
        let client = RestClient::new("http://localhost:8551/", Duration::from_secs(1)).unwrap();
        assert_eq!(client.url("/capabilities"), "http://localhost:8551/capabilities");
        assert_eq!(client.url("identity"), "http://localhost:8551/identity");
    }

    #[test]
    fn batch_responses_are_correlated_by_id() {
        let requests = vec![
            JsonRpcRequest { jsonrpc: "2.0", id: 10, method: "a".into(), params: json!([]) },
            JsonRpcRequest { jsonrpc: "2.0", id: 11, method: "b".into(), params: json!([]) },
        ];
        let reversed = vec![
            json!({"jsonrpc":"2.0","id":11,"result":"second"}),
            json!({"jsonrpc":"2.0","id":10,"result":"first"}),
        ];
        let ordered = order_batch_responses(&requests, &reversed).expect("valid batch");
        assert_eq!(ordered[0].id, Some(json!(10)));
        assert_eq!(ordered[1].id, Some(json!(11)));
    }

    #[test]
    fn rejects_missing_or_malformed_responses() {
        assert!(decode_response(json!({"jsonrpc":"2.0","id":1}), 1).is_err());
        assert!(decode_response(json!({"jsonrpc":"1.0","id":1,"result":null}), 1).is_err());
        assert!(decode_response(json!({"jsonrpc":"2.0","id":2,"result":null}), 1).is_err());
        assert!(decode_response(json!({"jsonrpc":"2.0","id":1,"result":null}), 1).is_ok());
    }
}
