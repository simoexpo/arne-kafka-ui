use crate::error::ApiError;
use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Response};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "../frontend/dist/"]
struct Assets;

const PLACEHOLDER: &str =
    "<!doctype html><html><body><h1>betrachtung</h1><p>frontend not built — run `npm run build` in frontend/ and rebuild.</p></body></html>";

/// Router-wide fallback for any request matching no declared route.
///
/// Owner ruling (review I3): `/api/*` is API surface, not a page — an
/// unmatched path there is a backend routing bug or a frontend typo, and
/// must answer with the same structured `ApiError` envelope every other API
/// failure uses, never the SPA's `text/html`. Everything else (a client-side
/// router path like `/topics/foo`) keeps serving the SPA shell so deep links
/// and refreshes work.
pub async fn spa_fallback(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    if uri.path() == "/api" || uri.path().starts_with("/api/") {
        return ApiError::not_found_route(uri.path()).into_response();
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    /// `/api` (no trailing slash) is unmatched API surface exactly like
    /// `/api/anything` — `starts_with("/api/")` alone misses this bare form
    /// and would serve the SPA shell instead of the 404 envelope.
    #[tokio::test]
    async fn bare_slash_api_gets_the_404_envelope_not_the_spa_shell() {
        let res = spa_fallback(Uri::from_static("/api")).await;
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
        let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["code"], "not_found");
    }
}
