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

use actix_web_httpauth::extractors::bearer::BearerAuth;

pub fn get(
    request: TokenServerRequest, auth: BearerAuth
) -> impl Future<Output = Result<HttpResponse, BlockingError<ApiError>>> {
    block(move || get_sync(request).map_err(Into::into)).map_ok(move |mut result| {
        result.id = auth.token().to_string();
        let body = serde_json::to_string(&result).unwrap();
        println!("BODY! {:}", body);
        HttpResponse::Ok()
            .content_type("application/json")
            .body(body)
    })
}

pub fn get_sync(_request: TokenServerRequest) -> Result<TokenServerResult, ApiError> {
    Ok(TokenServerResult {
        id: "id".to_string(),
        key: "key".to_string(),
        uid: "uid".to_string(),
        api_endpoint: "http://localhost:8000/".to_string(),
        duration: "10000000000".to_string(),
    })
}
