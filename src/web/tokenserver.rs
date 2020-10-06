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
use pyo3::prelude::*;
use pyo3::types::IntoPyDict;

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
        // the public rsa components come from
        // https://oauth.accounts.firefox.com/v1/jwks
        // TODO we should fetch it from there and cache it
        // instead of hardcoding it here.
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
    let python_result = Python::with_gil(|py| {
        let tokenlib = PyModule::from_code(py, r#"
import tokenlib
def make_token(plaintext, shared_secret):
    return tokenlib.make_token(plaintext, secret=shared_secret)
"#, "main.py", "main").map_err(|e| {
            e.print_and_set_sys_last_vars(py);
            e
        })?;

        let thedict = [("user_id", 42)].into_py_dict(py);
        let result = match tokenlib.call1("make_token", (thedict, "asdf",)) {
            Err(e) => {
                e.print_and_set_sys_last_vars(py);
                return Err(e)
            }
            Ok(x) => x.extract::<String>().unwrap(),
        };
        //assert_eq!(result, false);
        println!("result from python {:?}", result);
        Ok(result)
    }).unwrap();
    println!("python result {:}", python_result);
    Ok(TokenServerResult {
        id: "id".to_string(),
        key: python_result,
        uid: "uid".to_string(),
        api_endpoint: "http://localhost:8000/1.5/172434353/".to_string(),
        duration: "10000000000".to_string(),
    })
}
