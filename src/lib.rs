use async_trait::async_trait;
use s3::{Bucket, error::S3Error};
use tower_sessions::{
    CachingSessionStore, MemoryStore, SessionStore,
    session::{Id, Record},
    session_store::{self, Error},
};

#[derive(Debug)]
pub struct S3SessionStore {
    session_path: String,
    client: Box<Bucket>,
}

impl S3SessionStore {
    pub fn new(
        client: Box<Bucket>,
        session_path: Option<String>,
    ) -> CachingSessionStore<MemoryStore, Self> {
        let cache = MemoryStore::default();
        let session_path = session_path.unwrap_or_else(|| "/sessions".to_string());
        let store = Self {
            session_path,
            client,
        };

        CachingSessionStore::new(cache, store)
    }
}

#[async_trait]
impl SessionStore for S3SessionStore {
    async fn create(&self, session_record: &mut Record) -> session_store::Result<()> {
        while self
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
        let bytes = serde_zap::to_vec(&session_record).map_err(|e| Error::Encode(e.to_string()))?;
        self.client
            .put_object(make_path(&self.session_path, &session_record.id), &bytes)
            .await
            .map_err(|e| Error::Backend(e.to_string()))?;

        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
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
