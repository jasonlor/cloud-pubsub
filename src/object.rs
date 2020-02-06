use crate::client::Client;
use crate::error;
use hyper::Method;
use serde_derive::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct Object {
    pub bucket: String,
    pub name: String,

    #[serde(skip)]
    pub(crate) client: Option<Client>,
}

impl Object {
    pub async fn destroy(self) -> Result<(), error::Error> {
        let client = self.client.expect("Object was not created using a client");

        let uri: hyper::Uri = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
            self.bucket, self.name
        )
        .parse()
        .unwrap();

        let mut req = client.request(Method::DELETE, "");
        *req.uri_mut() = uri.clone();

        if let Err(e) = client.hyper_client().request(req).await {
            Err(e.into())
        } else {
            Ok(())
        }
    }
}
