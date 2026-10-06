use async_trait::async_trait;
use moka::future::Cache;
use s3::{Bucket, error::S3Error};
use tower_sessions::{
    SessionStore,
    session::{Id, Record},
    session_store::{self, Error},
};

#[derive(Debug)]
pub struct S3SessionStore {
    session_path: String,
    client: Box<Bucket>,
    cache: Cache<Id, Record>,
}

impl S3SessionStore {
    /// Setup a session store backed by an S3 bucket
    /// Bundled with a cache because S3 is slow
    pub fn new(client: Box<Bucket>, cache_limit: u64, session_path: Option<String>) -> Self {
        let session_path = session_path.unwrap_or_else(|| "/sessions".to_string());
        let cache = Cache::new(cache_limit);

        Self {
            session_path,
            client,
            cache,
        }
    }
}

#[async_trait]
impl SessionStore for S3SessionStore {
    async fn create(&self, session_record: &mut Record) -> session_store::Result<()> {
        while self.cache.contains_key(&session_record.id)
            || self
                .client
                .object_exists(&make_path(&self.session_path, &session_record.id))
                .await
                .map_err(backend_err)?
        {
            session_record.id = Id::default();
        }

        self.save(session_record).await
    }

    async fn save(&self, session_record: &Record) -> session_store::Result<()> {
        self.cache
            .insert(session_record.id, session_record.clone())
            .await;
        let bytes = serde_zap::to_vec(&session_record).map_err(|e| Error::Encode(e.to_string()))?;
        self.client
            .put_object(make_path(&self.session_path, &session_record.id), &bytes)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        if let Some(session) = self.cache.get(session_id).await {
            return Ok(Some(session));
        }

        let res = self
            .client
            .get_object(make_path(&self.session_path, &session_id))
            .await;
        match res {
            Ok(data) => {
                serde_zap::from_bytes(&data.into_bytes()).map_err(|e| Error::Decode(e.to_string()))
            }
            Err(err) => match err {
                S3Error::HttpFailWithBody(c, _) if c == 404 => Ok(None),
                err => Err(backend_err(err)),
            },
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        self.cache.remove(session_id).await;
        self.client
            .delete_object(make_path(&self.session_path, session_id))
            .await
            .map_err(backend_err)?;
        Ok(())
    }
}

fn make_path(session_path: &str, id: &Id) -> String {
    format!("{}/{:x}", session_path, id.0)
}

fn backend_err(err: S3Error) -> Error {
    Error::Backend(err.to_string())
}
