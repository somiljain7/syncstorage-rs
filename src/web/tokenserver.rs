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


#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct Claims {
    pub sub: String,
    pub iat: i64,
    pub exp: i64,
}

pub fn get(
    request: TokenServerRequest, auth: BearerAuth
) -> impl Future<Output = Result<HttpResponse, BlockingError<ApiError>>> {
    block(move || get_sync(request).map_err(Into::into)).map_ok(move |mut result| {
        //let pubkey_path = "".to_string();
        //result.id = auth.token().to_string();
        let claims = decode::<Claims>(
            &auth.token(),
            &DecodingKey::from_rsa_components("2lDphW0lNZ4w1m9CfmIhC1AxYG9iwihxBdQZo7_6e0TBAi8_TNaoHHI90G9n5d8BQQnNcF4j2vOs006zlXcqGrP27b49KkN3FmbcOMovvfesMseghaqXqqFLALL9us3Wstt_fV_qV7ceRcJq5Hd_Mq85qUgYSfb9qp0vyePb26KEGy4cwO7c9nCna1a_i5rzUEJu6bAtcLS5obSvmsOOpTLHXojKKOnC4LRC3osdR6AU6v3UObKgJlkk_-8LmPhQZqOXiI_TdBpNiw6G_-eishg8V_poPlAnLNd8mfZBam-_7CdUS4-YoOvJZfYjIoboOuVmUrBjogFyDo72EPTReQ", "AQAB"),
            &Validation::new(Algorithm::RS256),
        );

        let body = serde_json::to_string(&result).unwrap();
        println!("BODY! {:} {:?} {:?}", body, claims, auth.token());
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
