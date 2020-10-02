use actix_web::error::BlockingError;
use actix_web::web::block;
use actix_web::HttpResponse;

use futures::future::{Future, TryFutureExt};

use crate::error::ApiError;
use crate::web::extractors::TokenServerRequest;

use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{
    decode, encode, Algorithm, DecodingKey, EncodingKey, Header, TokenData, Validation,
};

#[derive(serde::Serialize)]
pub struct TokenServerResult {
    id: String,
    key: String,
    uid: String,
    api_endpoint: String,
    duration: String,
}

pub fn get(
    request: TokenServerRequest,
) -> impl Future<Output = Result<HttpResponse, BlockingError<ApiError>>> {
    block(move || get_sync(request).map_err(Into::into)).map_ok(move |result| {
        let body = serde_json::to_string(&result).unwrap();

        HttpResponse::Ok()
            .content_type("application/json")
            .body(body)
    })
}

pub fn get_sync(request: TokenServerRequest) -> Result<TokenServerResult, ApiError> {
    let something = request.
    Ok(TokenServerResult {
        id: "id".to_string(),
        key: "key".to_string(),
        uid: "uid".to_string(),
        api_endpoint: "http://localhost:8000/".to_string(),
        duration: "10000000000".to_string(),
    })
}
