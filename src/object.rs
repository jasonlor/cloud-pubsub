use crate::client::Client;
use crate::error;
use bytes::buf::BufExt as _;
use chrono::DateTime;
use chrono::Utc;
use hyper::Method;
use hyper::StatusCode;

use serde::de::DeserializeOwned;
use serde_derive::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Deserialize, Serialize)]
pub struct Object {
    pub bucket: String,
    pub name: String,

    #[serde(skip)]
    pub(crate) client: Option<Client>,
}

#[derive(Deserialize, Serialize)]
pub struct ObjectRequest {
    pub bucket: String,
    pub name: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ObjectResource {
    pub kind: String,
    pub id: String,
    pub self_link: String,
    pub name: String,
    pub bucket: String,
    pub generation: u64,
    pub metageneration: u64,
    pub content_type: String,
    pub time_created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    pub time_deleted: DateTime<Utc>,
    pub temporary_hold: bool,
    pub event_based_hold: bool,
    pub retention_expiration_time: DateTime<Utc>,
    pub storage_class: String,
    pub time_storage_class_updated: DateTime<Utc>,
    pub size: u64,
    pub md5_hash: String,
    pub media_link: String,
    pub content_encoding: String,
    pub content_disposition: String,
    pub content_language: String,
    pub cache_control: String,
    pub metadata: Option<HashMap<String, String>>,
    pub acl: Vec<ObjectAccessControl>,
    pub owner: ObjectOwner,
    pub crc32c: String,
    pub component_count: i32,
    pub etag: String,
    pub customer_encryption: ObjectCustomerEncryption,
    pub kms_key_name: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ObjectCustomerEncryption {
    pub encryption_algorithm: String,
    pub key_sha256: String,
}
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ObjectOwner {
    pub entity: String,
    pub entity_id: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ObjectAccessControl {
    pub kind: String,
    pub id: String,
    pub self_link: String,
    pub bucket: String,
    pub object: String,
    pub generation: u64,
    pub entity: String,
    pub role: String,
    pub email: String,
    pub entity_id: String,
    pub domain: String,
    pub project_team: ProjectTeam,
    pub etag: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ProjectTeam {
    pub project_number: String,
    pub team: String,
}

impl Object {
    pub async fn copy<T: serde::Serialize>(
        &self,
        destination: Object,
        data: T,
    ) -> Result<ObjectResource, error::Error> {
        let client = self.client.clone().expect("Object must be created using a client");

        let uri: hyper::Uri = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}/copyTo/b/{}/o/{}",
            self.bucket, self.name, destination.bucket, destination.name
        )
        .parse()
        .unwrap();

        let mut req = client.request(Method::POST, "");
        *req.uri_mut() = uri.clone();

        self.perform_request::<T, ObjectResource>(uri, Method::POST, data).await
    }

    pub async fn destroy(self) -> Result<(), error::Error> {
        let client = self.client.clone().expect("Object was not created using a client");

        let uri: hyper::Uri = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
            self.bucket, self.name
        )
        .parse()
        .unwrap();

        let mut req = client.request(Method::DELETE, "");
        *req.uri_mut() = uri.clone();

        self.perform_request::<&str, ObjectResource>(uri, Method::DELETE, "").await?;
        Ok(())
    }

    async fn perform_request<T: serde::Serialize, U: DeserializeOwned + Clone>(
        &self,
        uri: hyper::Uri,
        method: Method,
        data: T,
    ) -> Result<U, error::Error> {
        let client = self.client.clone().expect("Topic must be created using a client");

        let json =
            serde_json::to_string(&data).expect("Failed to serialize request body.");
        let mut req = client.request(method, json);
        *req.uri_mut() = uri;

        let response = client.hyper_client().request(req).await?;
        match response.status() {
            StatusCode::NOT_FOUND => Err(error::Error::PubSub {
                code: 404,
                status: "Object Not Found".to_string(),
                message: self.name.clone(),
            }),
            StatusCode::OK => {
                let body = hyper::body::aggregate(response).await?;
                serde_json::from_reader(body.reader()).map_err(|e| e.into())
            }
            code => {
                let body = hyper::body::aggregate(response).await?;
                let mut buf = String::new();
                use std::io::Read;
                body.reader().read_to_string(&mut buf)?;
                Err(error::Error::PubSub {
                    code: code.as_u16() as i32,
                    status: "Error occurred on object request".to_string(),
                    message: buf,
                })
            }
        }
    }
}
