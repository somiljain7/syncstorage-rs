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
    foo: String,
}

pub fn get(
    request: TokenServerRequest,
) -> impl Future<Output = Result<HttpResponse, BlockingError<ApiError>>> {
    block(move || get_sync(request).map_err(Into::into)).map_ok(move |_result| {
        // TODO turn _result into a json response.
        eprintln!("HELLO WORLD");
        let my_struct = TokenServerResult { foo: "bar".to_string() };
        let body = serde_json::to_string(&my_struct).unwrap();

        HttpResponse::Ok()
            .content_type("application/json")
            .body(body)
    })
}

pub fn get_sync(_request: TokenServerRequest) -> Result<TokenServerResult, ApiError> {
    Ok(TokenServerResult {foo: "bar".to_string() })
}
