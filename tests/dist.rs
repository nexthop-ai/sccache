#![cfg(all(feature = "dist-client", feature = "dist-server"))]

extern crate assert_cmd;
extern crate log;
extern crate sccache;
extern crate serde_json;

use crate::harness::{
    cargo_command, clear_cache_local_daemon, get_stats, init_cargo, sccache_command,
    start_local_daemon, stop_local_daemon, write_json_cfg, write_source,
};
use assert_cmd::prelude::*;
use sccache::config::HTTPUrl;
use sccache::dist::{
    self, AssignJobResult, CompileCommand, HeartbeatServerResult, InputsReader, JobId, JobState,
    RunJobResult, ServerIncoming, ServerNonce, ServerOutgoing, SubmitToolchainResult, Toolchain,
    ToolchainReader, UpdateJobStateResult,
};
use std::ffi::OsStr;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::str::FromStr;
use std::thread;
use std::time::Duration;

use sccache::errors::*;

mod harness;

fn basic_compile(tmpdir: &Path, sccache_cfg_path: &Path, sccache_cached_cfg_path: &Path) {
    let envs: Vec<(_, &OsStr)> = vec![
        ("RUST_BACKTRACE", "1".as_ref()),
        ("SCCACHE_LOG", "debug".as_ref()),
        ("SCCACHE_CONF", sccache_cfg_path.as_ref()),
        ("SCCACHE_CACHED_CONF", sccache_cached_cfg_path.as_ref()),
    ];
    let source_file = "x.c";
    let obj_file = "x.o";
    write_source(
        tmpdir,
        source_file,
        "#if !defined(SCCACHE_TEST_DEFINE)\n#error SCCACHE_TEST_DEFINE is not defined\n#endif\nint x() { return 5; }",
    );
    sccache_command()
        .args([
            std::env::var("CC")
                .unwrap_or_else(|_| "gcc".to_string())
                .as_str(),
            "-c",
            "-DSCCACHE_TEST_DEFINE",
        ])
        .arg(tmpdir.join(source_file))
        .arg("-o")
        .arg(tmpdir.join(obj_file))
        .envs(envs)
        .assert()
        .success();
}

fn rust_compile(tmpdir: &Path, sccache_cfg_path: &Path, sccache_cached_cfg_path: &Path) -> Output {
    let sccache_path = assert_cmd::cargo::cargo_bin("sccache").into_os_string();
    let envs: Vec<(_, &OsStr)> = vec![
        ("RUSTC_WRAPPER", sccache_path.as_ref()),
        ("CARGO_TARGET_DIR", "target".as_ref()),
        ("RUST_BACKTRACE", "1".as_ref()),
        ("SCCACHE_LOG", "debug".as_ref()),
        ("SCCACHE_CONF", sccache_cfg_path.as_ref()),
        ("SCCACHE_CACHED_CONF", sccache_cached_cfg_path.as_ref()),
    ];
    let cargo_name = "sccache-dist-test";
    let cargo_path = init_cargo(tmpdir, cargo_name);

    let manifest_file = "Cargo.toml";
    let source_file = "src/main.rs";

    write_source(
        &cargo_path,
        manifest_file,
        r#"[package]
        name = "sccache-dist-test"
        version = "0.1.0"
        edition = "2021"
        [dependencies]
        libc = "0.2.169""#,
    );
    write_source(
        &cargo_path,
        source_file,
        r#"fn main() {
        println!("Hello, world!");
}"#,
    );

    cargo_command()
        .current_dir(cargo_path)
        .args(["build", "--release"])
        .envs(envs)
        .output()
        .unwrap()
}

pub fn dist_test_sccache_client_cfg(
    tmpdir: &Path,
    scheduler_url: HTTPUrl,
) -> sccache::config::FileConfig {
    let mut sccache_cfg = harness::sccache_client_cfg(tmpdir, false);
    sccache_cfg.cache.disk.as_mut().unwrap().size = 0;
    sccache_cfg.dist.scheduler_url = Some(scheduler_url);
    sccache_cfg
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_basic() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    system.add_server();

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    get_stats(|info| {
        assert_eq!(1, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        assert_eq!(1, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_restartedserver() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    let server_handle = system.add_server();

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    system.restart_server(&server_handle);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    get_stats(|info| {
        assert_eq!(2, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        assert_eq!(2, info.stats.compile_requests);
        assert_eq!(2, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(2, info.stats.cache_misses.all());
    });
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_nobuilder() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    get_stats(|info| {
        assert_eq!(0, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(1, info.stats.dist_errors);
        assert_eq!(1, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}

struct FailingServer;
impl ServerIncoming for FailingServer {
    fn handle_assign_job(&self, _job_id: JobId, _tc: Toolchain) -> Result<AssignJobResult> {
        let need_toolchain = false;
        let state = JobState::Ready;
        Ok(AssignJobResult {
            need_toolchain,
            state,
        })
    }
    fn handle_submit_toolchain(
        &self,
        _requester: &dyn ServerOutgoing,
        _job_id: JobId,
        _tc_rdr: ToolchainReader,
    ) -> Result<SubmitToolchainResult> {
        panic!("should not have submitted toolchain")
    }
    fn handle_run_job(
        &self,
        requester: &dyn ServerOutgoing,
        job_id: JobId,
        _command: CompileCommand,
        _outputs: Vec<String>,
        _inputs_rdr: InputsReader,
    ) -> Result<RunJobResult> {
        requester
            .do_update_job_state(job_id, JobState::Started)
            .context("Updating job state failed")?;
        bail!("internal build failure")
    }
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_failingserver() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    system.add_custom_server(FailingServer);

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    get_stats(|info| {
        assert_eq!(0, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(1, info.stats.dist_errors);
        assert_eq!(1, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_cargo_build() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    let _server_handle = system.add_server();

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    rust_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path)
        .assert()
        .success();
    get_stats(|info| {
        assert_eq!(1, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        assert_eq!(8, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_cargo_makeflags() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    let _server_handle = system.add_server();

    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    let compile_output = rust_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    assert!(
        !String::from_utf8_lossy(&compile_output.stderr)
            .contains("warning: failed to connect to jobserver from environment variable")
    );

    get_stats(|info| {
        assert_eq!(1, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        assert_eq!(8, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_preprocesspr_cache_bug_2173() {
    // Bug 2173: preprocessor cache hit but main cache miss - because using the preprocessor cache
    // means not doing regular preprocessing, there was no preprocessed translation unit to send
    // out for distributed compilation, so an empty u8 array was compiled - which "worked", but
    // the object file *was* the result of compiling an empty file.
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    let _server_handle = system.add_server();

    let mut sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let disk_cache = sccache_cfg.cache.disk.as_mut().unwrap();
    disk_cache
        .preprocessor_cache_mode
        .use_preprocessor_cache_mode = true;
    disk_cache.size = 10_000_000; // enough for one tiny object file
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");

    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);

    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);
    let obj_file = "x.o";
    let obj_path = tmpdir.join(obj_file);
    let data_a = std::fs::read(&obj_path).unwrap();

    let cache_path = sccache_cfg.cache.disk.unwrap().dir;

    // Don't touch the preprocessor cache - and check that it exists
    assert!(
        cache_path.join("preprocessor").is_dir(),
        "The preprocessor cache should exist"
    );

    // Delete the main cache to ensure a cache miss - potential dirs are "0".."f".
    let main_cache_dirs = "0123456789abcdef";
    let delete_count = main_cache_dirs.chars().fold(0, |res, dir| {
        res + (std::fs::remove_dir_all(cache_path.join(String::from(dir))).is_ok() as u32)
    });
    assert_eq!(delete_count, 1, "Did the disk cache format change?");

    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    // Check that this gave the same result (i.e. that it didn't compile a completely empty file).
    // It would be nice to check directly that the object file contains the symbol for the x() function
    // from basic_compile(), but that seems pretty involved and this happens to work...
    let data_b = std::fs::read(&obj_path).unwrap();

    assert_eq!(data_a, data_b);
}

#[test]
#[cfg_attr(not(feature = "dist-tests"), ignore)]
fn test_dist_toolchain() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();
    let server_handle = system.add_server();
    let sccache_cfg = dist_test_sccache_client_cfg(tmpdir, system.scheduler_url());
    let sccache_cfg_path = tmpdir.join("sccache-cfg.json");
    write_json_cfg(tmpdir, "sccache-cfg.json", &sccache_cfg);
    let sccache_cached_cfg_path = tmpdir.join("sccache-cached-cfg");
    stop_local_daemon();
    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    stop_local_daemon();
    clear_cache_local_daemon(tmpdir);

    start_local_daemon(&sccache_cfg_path, &sccache_cached_cfg_path);
    basic_compile(tmpdir, &sccache_cfg_path, &sccache_cached_cfg_path);

    assert_eq!(system.count_toolchains_on_server(&server_handle), 1);

    get_stats(|info| {
        assert_eq!(1, info.stats.dist_compiles.values().sum::<usize>());
        assert_eq!(0, info.stats.dist_errors);
        assert_eq!(1, info.stats.compile_requests);
        assert_eq!(1, info.stats.requests_executed);
        assert_eq!(0, info.stats.cache_hits.all());
        assert_eq!(1, info.stats.cache_misses.all());
    });
}

#[cfg(feature = "dist-tests")]
mod nonce_race_test {
    use super::*;

    // A worker that blocks in handle_assign_job until a file appears, giving the
    // test a window to send a nonce-mismatch heartbeat to the scheduler.
    pub struct SlowServer {
        pub entered_path: PathBuf,
        pub proceed_path: PathBuf,
    }
    impl ServerIncoming for SlowServer {
        fn handle_assign_job(&self, _job_id: JobId, _tc: Toolchain) -> Result<AssignJobResult> {
            std::fs::File::create(&self.entered_path).unwrap();
            while !self.proceed_path.exists() {
                thread::sleep(Duration::from_millis(50));
            }
            Ok(AssignJobResult {
                state: JobState::Ready,
                need_toolchain: false,
            })
        }
        fn handle_submit_toolchain(
            &self,
            _requester: &dyn ServerOutgoing,
            _job_id: JobId,
            _tc_rdr: ToolchainReader,
        ) -> Result<SubmitToolchainResult> {
            panic!("should not have submitted toolchain")
        }
        fn handle_run_job(
            &self,
            requester: &dyn ServerOutgoing,
            job_id: JobId,
            _command: CompileCommand,
            _outputs: Vec<String>,
            _inputs_rdr: InputsReader,
        ) -> Result<RunJobResult> {
            requester
                .do_update_job_state(job_id, JobState::Started)
                .context("Updating job state failed")?;
            requester
                .do_update_job_state(job_id, JobState::Complete)
                .context("Updating job state failed")?;
            bail!("intentional failure after state updates")
        }
    }

    pub fn bincode_post<T: serde::Serialize, R: serde::de::DeserializeOwned>(
        client: &reqwest::blocking::Client,
        url: reqwest::Url,
        auth: &str,
        body: &T,
    ) -> Result<R> {
        let bytes = bincode::serialize(body).context("serialize")?;
        let res = client
            .post(url)
            .bearer_auth(auth)
            .header("content-type", "application/octet-stream")
            .header("content-length", bytes.len())
            .header("connection", "close")
            .body(bytes)
            .send()
            .context("send")?;
        if !res.status().is_success() {
            bail!("HTTP {}", res.status());
        }
        bincode::deserialize_from(res).map_err(Into::into)
    }
}

/// Demonstrates the race between handle_alloc_job's two-phase insert and a
/// nonce-mismatch heartbeat. When the worker's handle_assign_job is in flight
/// (scheduler has released its `servers` lock but not yet taken `jobs`), a
/// heartbeat with a new nonce replaces the ServerDetails (clearing
/// jobs_assigned). The scheduler then inserts into self.jobs, leaving the two
/// maps permanently inconsistent. The next Complete transition panics at
/// `assert!(entry.jobs_assigned.remove(&job_id))`, poisoning both mutexes.
#[cfg(feature = "dist-tests")]
#[test]
fn test_dist_worker_restart_during_alloc() {
    let tmpdir = tempfile::Builder::new()
        .prefix("sccache_dist_race_test")
        .tempdir()
        .unwrap();
    let tmpdir = tmpdir.path();
    let sccache_dist = harness::sccache_dist_path();

    let mut system = harness::DistSystem::new(&sccache_dist, tmpdir);
    system.add_scheduler();

    let entered_path = tmpdir.join("assign_entered");
    let proceed_path = tmpdir.join("assign_proceed");

    let slow_server = nonce_race_test::SlowServer {
        entered_path: entered_path.clone(),
        proceed_path: proceed_path.clone(),
    };
    let server_handle = system.add_custom_server(slow_server);
    let server_addr = match &server_handle {
        harness::ServerHandle::Process { url, .. } => {
            let u = url.to_url();
            SocketAddr::from_str(&format!("{}:{}", u.host_str().unwrap(), u.port().unwrap()))
                .unwrap()
        }
        _ => panic!("expected Process handle"),
    };
    let server_id = dist::ServerId::new(server_addr);
    let server_token = harness::create_server_token(server_id, harness::DIST_SERVER_TOKEN);
    let scheduler_url = system.scheduler_url().to_url();

    // Spawn a thread to POST alloc_job — it will block while the worker's
    // handle_assign_job is parked.
    let alloc_url = dist::http::urls::scheduler_alloc_job(&scheduler_url);
    let alloc_handle = thread::spawn(
        move || -> Result<dist::http::common::AllocJobHttpResponse> {
            let client = reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(60))
                .build()
                .unwrap();
            let tc = Toolchain {
                archive_id: "tc".into(),
            };
            nonce_race_test::bincode_post(
                &client,
                alloc_url,
                sccache::config::INSECURE_DIST_CLIENT_TOKEN,
                &tc,
            )
        },
    );

    // Wait until the worker has entered handle_assign_job — at this point the
    // scheduler has inserted into server.jobs_assigned and dropped the servers
    // lock, but hasn't yet inserted into self.jobs.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !entered_path.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for handle_assign_job"
        );
        thread::sleep(Duration::from_millis(50));
    }

    // Send a heartbeat with a DIFFERENT nonce for the same server_id. The
    // scheduler sees a nonce mismatch, wipes the old ServerDetails (including
    // jobs_assigned), and registers a new one with empty state.
    let (cert_digest, cert_pem, _privkey_pem) =
        dist::http::create_https_cert_and_privkey(server_addr)
            .expect("failed to create cert for fake heartbeat");
    let heartbeat = dist::http::common::HeartbeatServerHttpRequest {
        jwt_key: vec![0; 64],
        num_cpus: 4,
        server_nonce: ServerNonce::new(),
        cert_digest,
        cert_pem,
    };
    let hb_url = dist::http::urls::scheduler_heartbeat_server(&scheduler_url);
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let hb_result: HeartbeatServerResult =
        nonce_race_test::bincode_post(&client, hb_url, &server_token, &heartbeat)
            .expect("heartbeat failed");
    assert!(
        hb_result.is_new,
        "expected nonce-mismatch to register as new server"
    );

    // Let the worker proceed so do_assign_job returns Ok to the scheduler.
    // The scheduler should detect that jobs_assigned was cleared by the
    // nonce-mismatch heartbeat and return CommunicationError instead of
    // inserting into self.jobs (which would desync the two maps).
    std::fs::File::create(&proceed_path).unwrap();

    let alloc_resp = alloc_handle.join().unwrap().expect("alloc_job HTTP failed");
    match alloc_resp {
        dist::http::common::AllocJobHttpResponse::CommunicationError { .. } => {
            eprintln!("alloc_job correctly returned CommunicationError");
        }
        dist::http::common::AllocJobHttpResponse::Success { .. } => {
            panic!(
                "expected CommunicationError, got Success — scheduler inserted into \
                    self.jobs despite jobs_assigned being cleared"
            );
        }
        dist::http::common::AllocJobHttpResponse::Fail { msg } => {
            panic!("unexpected Fail: {msg}");
        }
    };

    // The scheduler must still be alive and responsive — no panics, no
    // poisoned mutexes.
    let status_url = dist::http::urls::scheduler_status(&scheduler_url);
    let status_resp = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
        .get(status_url)
        .send()
        .expect("scheduler should still be reachable");
    assert!(
        status_resp.status().is_success(),
        "scheduler should still respond to status after the race"
    );
}
