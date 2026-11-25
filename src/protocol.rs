use crate::compiler::ColorMode;
use crate::server::{DistInfo, ServerInfo};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;

/// A client request.
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// Zero the server's statistics.
    ZeroStats,
    /// Get server statistics.
    GetStats,
    /// Get dist status.
    DistStatus,
    /// Shut the server down gracefully.
    Shutdown,
    /// Execute a compile or fetch a cached compilation result.
    Compile(Compile),
}

/// A server response.
#[derive(Serialize, Deserialize, Debug)]
pub enum Response {
    /// Response for `Request::Compile`.
    Compile(CompileResponse),
    /// Response for `Request::ZeroStats`.
    ZeroStats,
    /// Response for `Request::GetStats`, containing server statistics.
    Stats(Box<ServerInfo>),
    /// Response for `Request::DistStatus`, containing client info.
    DistStatus(DistInfo),
    /// Response for `Request::Shutdown`, containing server statistics.
    ShuttingDown(Box<ServerInfo>),
    /// Second response for `Request::Compile`, containing the results of the compilation.
    CompileFinished(CompileFinished),
}

/// Possible responses from the server for a `Compile` request.
#[derive(Serialize, Deserialize, Debug)]
pub enum CompileResponse {
    /// The compilation was started.
    /// The `remote_only` field indicates whether remote-only mode is active.
    /// When true, the client should not fall back to local compilation on errors.
    CompileStarted {
        #[serde(default)]
        remote_only: bool,
    },
    /// The server could not handle this compilation request.
    UnhandledCompile,
    /// The compiler was not supported.
    UnsupportedCompiler(OsString),
}

/// Information about a finished compile, either from cache or executed locally.
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CompileFinished {
    /// The return code of the compile process, if available.
    pub retcode: Option<i32>,
    /// The signal that terminated the compile process, if available.
    pub signal: Option<i32>,
    /// The compiler's stdout.
    pub stdout: Vec<u8>,
    /// The compiler's stderr.
    pub stderr: Vec<u8>,
    /// The state of any compiler options passed to control color output.
    pub color_mode: ColorMode,
}

/// The contents of a compile request from a client.
#[derive(Serialize, Deserialize, Debug)]
pub struct Compile {
    /// The full path to the compiler executable.
    pub exe: OsString,
    /// The current working directory in which to execute the compile.
    pub cwd: OsString,
    /// The commandline arguments passed to the compiler.
    pub args: Vec<OsString>,
    /// The environment variables present when the compiler was executed, as (var, val).
    pub env_vars: Vec<(OsString, OsString)>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_compile_started_remote_only_round_trip() {
        let bytes =
            bincode::serialize(&CompileResponse::CompileStarted { remote_only: true }).unwrap();
        let response: CompileResponse = bincode::deserialize(&bytes).unwrap();
        assert!(matches!(
            response,
            CompileResponse::CompileStarted { remote_only: true }
        ));
    }

    /// A server without the `remote_only` field sends `CompileStarted` as a
    /// unit variant. bincode is not self-describing, so `#[serde(default)]`
    /// has no effect and a newer client cannot read it.
    #[test]
    fn test_compile_started_without_remote_only_field_fails() {
        #[derive(Serialize)]
        enum OldCompileResponse {
            CompileStarted,
        }
        let bytes = bincode::serialize(&OldCompileResponse::CompileStarted).unwrap();
        assert!(bincode::deserialize::<CompileResponse>(&bytes).is_err());
    }
}
