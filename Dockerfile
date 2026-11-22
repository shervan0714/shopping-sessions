# Shopping Sessions: one static native Rust binary, in two builds.
# The scorers build this file natively on linux/amd64 (Google Cloud n2: Intel
# Xeon, Cascade Lake or newer, AVX-512), so no FROM line pins a platform.

# rustc 1.85.1 exactly: pgo/merged.profdata was made by this compiler
# (scripts/pgo.sh) and must match its LLVM.
FROM rust:1.85.1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY pgo ./pgo
# Two builds, both statically linked (glibc +crt-static: no dynamic loader
# work at start) and both optimised with the profile:
#   shopping-sessions         x86-64-v3 (AVX2/BMI2/FMA): runs on any scorer
#   shopping-sessions-avx512  Cascade Lake (adds AVX-512)
# At startup the first checks the CPU (CPUID) and runs the second only if
# every feature it may use is there; if that build fails in any way, the
# first clears its output and does the work itself (src/main.rs,
# try_avx512_build).
# An arm64 build (Apple-silicon dev loop) gets one plain static binary.
RUN set -eu; \
    T="$(uname -m)-unknown-linux-gnu"; \
    STATIC="-C target-feature=+crt-static"; \
    PGO="-Cprofile-use=/src/pgo/merged.profdata -Cllvm-args=-no-pgo-warn-mismatch"; \
    mkdir -p /out; \
    if [ "$(uname -m)" = "x86_64" ]; then \
      RUSTFLAGS="-C target-cpu=x86-64-v3 $STATIC $PGO" \
        cargo build --release --locked --target "$T"; \
      cp "target/$T/release/shopping-sessions" /out/shopping-sessions; \
      RUSTFLAGS="-C target-cpu=cascadelake $STATIC $PGO" CARGO_TARGET_DIR=target-avx512 \
        cargo build --release --locked --target "$T"; \
      cp "target-avx512/$T/release/shopping-sessions" /out/shopping-sessions-avx512; \
    else \
      RUSTFLAGS="$STATIC" cargo build --release --locked --target "$T"; \
      cp "target/$T/release/shopping-sessions" /out/shopping-sessions; \
    fi; \
    strip /out/*

# The binaries are static (no loader, no shared libraries at start), so the
# base image costs nothing at run time. It is a small OS rather than `scratch`
# because scripts/docker_run.sh runs `chmod` inside the image to clean up.
FROM debian:bookworm-slim
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /out/ /app/
# At run time the root filesystem is read-only, there is no network, /data is
# read-only and only /output, /scratch (and a 64 MB /tmp) are writable. The
# binary writes nothing but the two output folders and logs to stderr.
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch TMPDIR=/scratch
USER 1000:1000
WORKDIR /app
CMD ["/app/shopping-sessions"]
