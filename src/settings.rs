use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::fmt::Formatter;

#[derive(Clone, Serialize, Deserialize)]
pub struct RedisSettings {
    /// network address of the Redis service. Can be "tcp://127.0.0.1:6379", e.g.
    ///
    /// default is "tcp://127.0.0.1:6379"
    pub address: Option<String>,
    /// network address of the Redis cluster service. Can be "tcp://127.0.0.1:6379,tcp://127.0.0.1:6380,tcp://127.0.0.1:6381", e.g.
    ///
    /// default is None
    pub addresses: Option<String>,
    /// the username to connect redis service.
    ///
    /// default is None
    pub username: Option<String>,
    /// the password for authentication
    ///
    /// default is None
    pub password: Option<String>,
    // /// the working directory of the Redis service. Can be "/path/to/dir"
    // ///
    // /// default is "/"
    // root: Option<String>,
    /// the number of DBs redis can take is unlimited
    ///
    /// default is db 0
    pub db: i64,
}

impl Debug for RedisSettings {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("RedisSettings");

        d.field("db", &self.db.to_string());
        if let Some(address) = self.address.clone() {
            d.field("address", &address);
        }
        if let Some(addresses) = self.addresses.clone() {
            d.field("cluster_endpoints", &addresses);
        }
        if let Some(username) = self.username.clone() {
            d.field("username", &username);
        }
        if self.password.is_some() {
            d.field("password", &"<redacted>");
        }

        d.finish_non_exhaustive()
    }
}
