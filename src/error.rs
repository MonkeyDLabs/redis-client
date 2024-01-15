//! Errors that returned by OpenDAL
//!
//! # Examples
//!
//! ```
//! # use anyhow::Result;
//! use redis::ErrorKind;
//! # #[tokio::main]
//! # async fn test() -> Result<()> {
//! if let Err(e) = my_fn().await {
//!     if e.kind() == ErrorKind::ConfigInvalid {
//!         println!("config is invalid")
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!

use std::backtrace::Backtrace;
use std::backtrace::BacktraceStatus;
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

/// Result that is a wrapper of `Result<T, redis::Error>`
pub type Result<T> = std::result::Result<T, Error>;

/// ErrorKind is all kinds of Error of Redis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// Redis don't know what happened here, and no actions other than just
    /// returning it back.
    Unexpected,  
    /// The config for backend is invalid.
    ConfigInvalid,
}

impl ErrorKind {
    /// Convert self into static str.
    pub fn into_static(self) -> &'static str {
        self.into()
    }
}

impl Display for ErrorKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.into_static())
    }
}


impl From<ErrorKind> for &'static str {
  fn from(v: ErrorKind) -> &'static str {
      match v {
          ErrorKind::Unexpected => "Unexpected",
          ErrorKind::ConfigInvalid => "ConfigInvalid",
      }
  }
}

/// Error is the error struct returned by redis functions.
///
/// ## Display
///
/// Error can be displayed in two ways:
///
/// - Via `Display`: like `err.to_string()` or `format!("{err}")`
///
/// Error will be printed in a single line:
///
/// ```shell
/// Unexpected, context: { path: /path/to/file, called: send_async } => something wrong happened, source: networking error"
/// ```
///
/// - Via `Debug`: like `format!("{err:?}")`
///
/// Error will be printed in multi lines with more details and backtraces (if captured):
///
/// ```shell
/// Unexpected => something wrong happened
///
/// Context:
///    path: /path/to/file
///    called: send_async
///
/// Source: networking error
///
/// Backtrace:
///    0: redis::error::Error::new
///              at ./src/error.rs:197:24
///    1: redis::error::tests::generate_error
///              at ./src/error.rs:241:9
///    2: redis::error::tests::test_error_debug_with_backtrace::{{closure}}
///              at ./src/error.rs:305:41
///    ...
/// ```
///
/// - For conventional struct-style Debug representation, like `format!("{err:#?}")`:
///
/// ```shell
/// Error {
///     kind: Unexpected,
///     message: "something wrong happened",
///     source: Some(
///         "networking error",
///     ),
/// }
/// ```
pub struct Error {
    kind: ErrorKind,
    message: String,

    context: Vec<(&'static str, String)>,
    source: Option<anyhow::Error>,
    backtrace: Backtrace,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;

        if !self.context.is_empty() {
            write!(f, ", context: {{ ")?;
            write!(
                f,
                "{}",
                self.context
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )?;
            write!(f, " }}")?;
        }

        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }

        if let Some(source) = &self.source {
            write!(f, ", source: {source}")?;
        }

        Ok(())
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // If alternate has been specified, we will print like Debug.
        if f.alternate() {
            let mut de = f.debug_struct("Error");
            de.field("kind", &self.kind);
            de.field("message", &self.message);
            de.field("context", &self.context);
            de.field("source", &self.source);
            return de.finish();
        }

        write!(f, "{}", self.kind)?;
        if !self.message.is_empty() {
            write!(f, " => {}", self.message)?;
        }
        writeln!(f)?;

        if !self.context.is_empty() {
            writeln!(f)?;
            writeln!(f, "Context:")?;
            for (k, v) in self.context.iter() {
                writeln!(f, "   {k}: {v}")?;
            }
        }
        if let Some(source) = &self.source {
            writeln!(f)?;
            writeln!(f, "Source:")?;
            writeln!(f, "   {source:#}")?;
        }
        if self.backtrace.status() == BacktraceStatus::Captured {
            writeln!(f)?;
            writeln!(f, "Backtrace:")?;
            writeln!(f, "{}", self.backtrace)?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|v| v.as_ref())
    }
}

impl Error {
    /// Create a new Error with error kind and message.
    pub fn new(kind: ErrorKind, message: &str) -> Self {
        Self {
            kind,
            message: message.to_string(),
            context: Vec::default(),
            source: None,
            // `Backtrace::capture()` will check if backtrace has been enabled
            // internally. It's zero cost if backtrace is disabled.
            backtrace: Backtrace::capture(),
        }
    }
    /// Add more context in error.
    pub fn with_context(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.context.push((key, value.into()));
        self
    }

    /// Set source for error.
    ///
    /// # Notes
    ///
    /// If the source has been set, we will raise a panic here.
    pub fn set_source(mut self, src: impl Into<anyhow::Error>) -> Self {
        debug_assert!(self.source.is_none(), "the source error has been set");

        self.source = Some(src.into());
        self
    }
}
