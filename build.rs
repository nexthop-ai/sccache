// Build script that embeds a `git describe`-style string into the binary at
// compile time, exposed via the `SCCACHE_GIT_DESCRIBE` environment variable
// (see `env!("SCCACHE_GIT_DESCRIBE")` usages).
//
// This fork doesn't bump `Cargo.toml`'s `version` for every release; instead
// releases are cut by pushing a `vN` tag on GitHub (see
// `.github/workflows/release-latest.yml`). Embedding the tag/commit info
// here lets `--version` identify exactly which release a binary came from.

use std::process::Command;

fn main() {
    println!("cargo:rustc-env=SCCACHE_GIT_DESCRIBE={}", git_describe());

    // Allow CI (or anyone else) to short-circuit the `git describe` call
    // below by providing the answer directly, e.g. when building inside a
    // container image that lacks `git` and/or a full `.git` checkout (see
    // `.github/workflows/release-latest.yml`, which sets this to the pushed
    // tag name).
    println!("cargo:rerun-if-env-changed=SCCACHE_GIT_DESCRIBE_OVERRIDE");

    // Re-run this build script whenever the current commit or any ref
    // (branch/tag) changes, so the embedded version stays accurate.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/refs");
    println!("cargo:rerun-if-changed=.git/packed-refs");
}

/// Returns something like `v11-6-gf90b2cc` (`<tag>-<commits-since>-g<sha>`),
/// with a `-modified` suffix if the working tree has uncommitted changes.
///
/// If `SCCACHE_GIT_DESCRIBE_OVERRIDE` is set in the build environment, it is
/// used verbatim instead of invoking `git`. This is needed for release
/// builds that happen inside cross-compilation containers which may not
/// have `git` installed, or may have been checked out without a `.git`
/// directory (e.g. via `actions/checkout`'s no-git fallback).
///
/// Otherwise falls back to `"unknown"` if `git` isn't available or this
/// isn't a git checkout (e.g. building from a source tarball).
fn git_describe() -> String {
    if let Ok(over) = std::env::var("SCCACHE_GIT_DESCRIBE_OVERRIDE") {
        let over = over.trim().to_string();
        if !over.is_empty() {
            return over;
        }
    }

    Command::new("git")
        .args(["describe", "--tags", "--always", "--dirty=-modified"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".to_string())
}
