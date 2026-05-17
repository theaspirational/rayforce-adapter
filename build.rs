use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

fn run(cmd: &str, args: &[&str], dir: &Path, label: &str) {
    let status = Command::new(cmd)
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap_or_else(|err| panic!("{label}: failed to run `{cmd}`: {err}"));
    if !status.success() {
        panic!("{label}: `{cmd} {}` failed", args.join(" "));
    }
}

fn locate_rayforce(manifest_dir: &Path) -> PathBuf {
    if let Some(dir) = env::var_os("RAYFORCE_DIR").map(PathBuf::from) {
        return dir;
    }

    let sibling = manifest_dir.join("../rayforce");
    if sibling.join("Makefile").exists() {
        return sibling;
    }

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let cloned = out_dir.join("rayforce");
    const RAYFORCE_REPO: &str = "https://github.com/RayforceDB/rayforce.git";
    const RAYFORCE_REF: &str = "master";

    if !cloned.join("Makefile").exists() {
        eprintln!("[rayforce-adapter build] fetching rayforce {RAYFORCE_REF}");
        let _ = std::fs::remove_dir_all(&cloned);
        std::fs::create_dir_all(&cloned).expect("create rayforce clone dir");
        run("git", &["init"], &cloned, "rayforce init");
        run(
            "git",
            &["remote", "add", "origin", RAYFORCE_REPO],
            &cloned,
            "rayforce remote add",
        );
        run(
            "git",
            &["fetch", "--depth", "1", "origin", RAYFORCE_REF],
            &cloned,
            "rayforce fetch",
        );
        run(
            "git",
            &["checkout", "--detach", "FETCH_HEAD"],
            &cloned,
            "rayforce checkout",
        );
    }

    cloned
}

fn main() {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let rayforce_dir = locate_rayforce(&manifest_dir);

    println!("cargo:rerun-if-env-changed=RAYFORCE_DIR");
    for path in [
        "Makefile",
        "include/rayforce.h",
        "src/store/splay.c",
        "src/store/col.c",
        "src/table/sym.c",
        "src/vec/str.c",
    ] {
        println!(
            "cargo:rerun-if-changed={}",
            rayforce_dir.join(path).display()
        );
    }

    run("make", &["lib"], &rayforce_dir, "rayforce build");

    println!("cargo:rustc-link-search=native={}", rayforce_dir.display());
    println!("cargo:rustc-link-lib=static=rayforce");
    println!("cargo:rustc-link-lib=m");
    println!("cargo:rustc-link-lib=pthread");
}
