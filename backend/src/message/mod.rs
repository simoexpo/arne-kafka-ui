pub mod avro;
pub mod decode;
pub mod fetch;
pub mod filter;
pub mod proto;
pub mod range;
pub mod schema_registry;
pub mod tail;
pub mod timeline;

use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Encoding {
    Avro,
    Protobuf,
    Json,
    Utf8,
    Bytes,
    DecodeError,
}

#[derive(Debug, Serialize)]
pub struct DecodedPayload {
    pub encoding: Encoding,
    pub text: String,
    pub schema_id: Option<i32>,
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HeaderOut {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize)]
pub struct MessageOut {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub key: Option<DecodedPayload>,
    pub value: Option<DecodedPayload>,
    pub headers: Vec<HeaderOut>,
}
