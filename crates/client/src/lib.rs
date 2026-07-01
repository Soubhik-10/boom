use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use common::{JsonRpcRequest, JsonRpcResponse};
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    fs,
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone)]
pub struct JsonRpcClient {
    url: String,
    http: reqwest::Client,
    jwt: Option<JwtSigner>,
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
        Ok(Self { url: url.into(), http, jwt: None })
    }

    pub fn with_jwt(mut self, signer: JwtSigner) -> Self {
        self.jwt = Some(signer);
        self
    }

    pub async fn call(
        &self,
        id: u64,
        method: impl Into<String>,
        params: Value,
    ) -> Result<JsonRpcResponse> {
        let request = JsonRpcRequest { jsonrpc: "2.0", id, method: method.into(), params };
        let mut responses = self.send_json(&request).await?;
        serde_json::from_value(responses.remove(0))
            .with_context(|| "decoding JSON-RPC response".to_string())
    }

    pub async fn call_batch(&self, requests: &[JsonRpcRequest]) -> Result<Vec<JsonRpcResponse>> {
        let values = self.send_json(requests).await?;
        values
            .into_iter()
            .map(|value| serde_json::from_value(value).context("decoding JSON-RPC batch response"))
            .collect()
    }

    async fn send_json<T: serde::Serialize + ?Sized>(&self, payload: &T) -> Result<Vec<Value>> {
        let mut headers = HeaderMap::new();
        if let Some(jwt) = &self.jwt {
            let value = format!("Bearer {}", jwt.token()?);
            headers.insert(AUTHORIZATION, HeaderValue::from_str(&value)?);
        }

        let response = self.http.post(&self.url).headers(headers).json(payload).send().await?;
        let status = response.status();
        let body = response.text().await?;
        if !status.is_success() {
            return Err(anyhow!("HTTP {status}: {body}"));
        }
        let value: Value = serde_json::from_str(&body)
            .with_context(|| format!("decoding JSON-RPC response body: {body}"))?;
        Ok(match value {
            Value::Array(values) => values,
            other => vec![other],
        })
    }
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
    for chunk in bytes.chunks_exact(2) {
        let hi = from_hex(chunk[0])?;
        let lo = from_hex(chunk[1])?;
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
}
