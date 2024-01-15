// This code is an adaptation of the code from
// https://github.com/apache/incubator-opendal/blob/main/core/src/services/redis/backend.rs

use http::Uri;
use redis::aio::ConnectionManager;
use redis::cluster::ClusterClient;
use redis::cluster::ClusterClientBuilder;
use redis::cluster_async::ClusterConnection;
use redis::{
    AsyncCommands, Client, ConnectionAddr, ConnectionInfo, RedisConnectionInfo, RedisError,
};
use std::fmt::Debug;
use std::fmt::Formatter;
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::OnceCell;

use crate::error::Error;
use crate::error::ErrorKind;
use crate::error::Result;
use crate::settings::RedisSettings;

const DEFAULT_REDIS_ENDPOINT: &str = "tcp://127.0.0.1:6379";
const DEFAULT_REDIS_PORT: u16 = 6379;

fn format_redis_error(e: RedisError) -> Error {
    Error::new(ErrorKind::Unexpected, e.category()).set_source(e)
}

fn get_connection_info(endpoint: String, settings: &RedisSettings) -> Result<ConnectionInfo> {
    let ep_url = endpoint.parse::<Uri>().map_err(|e| {
        Error::new(ErrorKind::ConfigInvalid, "endpoint is invalid")
            .with_context("endpoint", endpoint)
            .set_source(e)
    })?;

    let con_addr = match ep_url.scheme_str() {
        Some("tcp") | Some("redis") | None => {
            let host = ep_url
                .host()
                .map(|h| h.to_string())
                .unwrap_or_else(|| "127.0.0.1".to_string());
            let port = ep_url.port_u16().unwrap_or(DEFAULT_REDIS_PORT);
            ConnectionAddr::Tcp(host, port)
        }
        Some("rediss") => {
            let host = ep_url
                .host()
                .map(|h| h.to_string())
                .unwrap_or_else(|| "127.0.0.1".to_string());
            let port = ep_url.port_u16().unwrap_or(DEFAULT_REDIS_PORT);
            ConnectionAddr::TcpTls {
                host,
                port,
                insecure: false,
                tls_params: None,
            }
        }
        Some("unix") | Some("redis+unix") => {
            let path = PathBuf::from(ep_url.path());
            ConnectionAddr::Unix(path)
        }
        Some(_) => {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "invalid or unsupported scheme",
            ))
        }
    };

    let redis_info = RedisConnectionInfo {
        db: settings.db,
        username: settings.username.clone(),
        password: settings.password.clone(),
    };

    Ok(ConnectionInfo {
        addr: con_addr,
        redis: redis_info,
    })
}

#[derive(Clone)]
enum RedisConnection {
    Single(ConnectionManager),
    Cluster(ClusterConnection),
}

#[derive(Clone)]
pub struct RedisClient {
    addresses: String,
    client: Option<Client>,
    cluster_client: Option<ClusterClient>,
    conn: OnceCell<RedisConnection>,
}

// implement `Debug` manually, or password may be leaked.
impl Debug for RedisClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("RedisClient");

        ds.field("addresses", &self.addresses);
        ds.finish()
    }
}

impl RedisClient {
    pub fn new(settings: &RedisSettings) -> Result<Self> {
        if let Some(addresses) = settings.addresses.clone() {
            let mut cluser_addresses: Vec<ConnectionInfo> = Vec::default();
            for address in addresses.split(",") {
                cluser_addresses.push(get_connection_info(address.to_string(), settings)?);
            }

            let mut client_builder = ClusterClientBuilder::new(cluser_addresses);
            if let Some(username) = &settings.username {
                client_builder = client_builder.username(username.clone());
            }
            if let Some(password) = &settings.password {
                client_builder = client_builder.password(password.clone());
            }
            let client = client_builder.build().map_err(format_redis_error)?;

            Ok(Self {
                addresses,
                client: None,
                cluster_client: Some(client),
                conn: OnceCell::new(),
            })
        } else {
            let address = settings
                .address
                .clone()
                .unwrap_or_else(|| DEFAULT_REDIS_ENDPOINT.to_string());

            let client =
                Client::open(get_connection_info(address.clone(), settings)?).map_err(|e| {
                    Error::new(ErrorKind::ConfigInvalid, "invalid or unsupported scheme")
                        .with_context("address", &address)
                        .with_context("db", settings.db.to_string())
                        .set_source(e)
                })?;

            Ok(Self {
                addresses: address,
                client: Some(client),
                cluster_client: None,
                conn: OnceCell::new(),
            })
        }
    }

    async fn connect(&self) -> Result<RedisConnection> {
        Ok(self
            .conn
            .get_or_try_init(|| async {
                if let Some(client) = self.client.clone() {
                    ConnectionManager::new(client.clone())
                        .await
                        .map(RedisConnection::Single)
                } else {
                    self.cluster_client
                        .clone()
                        .unwrap()
                        .get_async_connection()
                        .await
                        .map(RedisConnection::Cluster)
                }
            })
            .await
            .map_err(format_redis_error)?
            .clone())
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.connect().await?;
        match conn {
            RedisConnection::Single(mut conn) => {
                let bs = conn.get(key).await.map_err(format_redis_error)?;
                Ok(bs)
            }
            RedisConnection::Cluster(mut conn) => {
                let bs = conn.get(key).await.map_err(format_redis_error)?;
                Ok(bs)
            }
        }
    }

    pub async fn set(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<()> {
        let conn = self.connect().await?;
        match ttl {
            Some(ttl) => match conn {
                RedisConnection::Single(mut conn) => conn
                    .set_ex(key, value, ttl.as_secs())
                    .await
                    .map_err(format_redis_error)?,
                RedisConnection::Cluster(mut conn) => conn
                    .set_ex(key, value, ttl.as_secs())
                    .await
                    .map_err(format_redis_error)?,
            },
            None => match conn {
                RedisConnection::Single(mut conn) => {
                    conn.set(key, value).await.map_err(format_redis_error)?
                }
                RedisConnection::Cluster(mut conn) => {
                    conn.set(key, value).await.map_err(format_redis_error)?
                }
            },
        }
        Ok(())
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        let conn = self.connect().await?;
        match conn {
            RedisConnection::Single(mut conn) => {
                let _: () = conn.del(key).await.map_err(format_redis_error)?;
            }
            RedisConnection::Cluster(mut conn) => {
                let _: () = conn.del(key).await.map_err(format_redis_error)?;
            }
        }
        Ok(())
    }

    pub async fn append(&self, key: &str, value: &[u8]) -> Result<()> {
        let conn = self.connect().await?;
        match conn {
            RedisConnection::Single(mut conn) => {
                conn.append(key, value).await.map_err(format_redis_error)?;
            }
            RedisConnection::Cluster(mut conn) => {
                conn.append(key, value).await.map_err(format_redis_error)?;
            }
        }
        Ok(())
    }
}
