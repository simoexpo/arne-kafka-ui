use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../frontend/dist/"]
struct Assets;

const PLACEHOLDER: &str =
    "<!doctype html><html><body><h1>betrachtung</h1><p>frontend not built — run `npm run build` in frontend/ and rebuild.</p></body></html>";

pub async fn spa_fallback(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if !path.is_empty()
        && let Some(asset) = Assets::get(path)
    {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref().to_string())], asset.data).into_response();
    }
    match Assets::get("index.html") {
        Some(index) => Html(index.data).into_response(),
        None => (StatusCode::OK, Html(PLACEHOLDER)).into_response(),
    }
}
