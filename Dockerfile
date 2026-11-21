# Shopping Sessions: a single native Rust binary.
# The scorers build this file natively on linux/amd64 (Intel Xeon, Cascade
# Lake or newer), so no FROM line pins a platform.

FROM rust:1.85-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# x86-64-v3 = AVX2/BMI2/FMA: every Cascade Lake or newer Xeon has it.
# Only applied on x86_64 so an arm64 build (Apple-silicon dev loop) still works.
RUN if [ "$(uname -m)" = "x86_64" ]; then export RUSTFLAGS="-C target-cpu=x86-64-v3"; fi \
 && cargo build --release --locked \
 && strip target/release/shopping-sessions

FROM debian:bookworm-slim
# The scorer runs the container as uid 1000; give that uid a name and a home.
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
# At run time the root filesystem is read-only, there is no network, /data is
# read-only and only /output, /scratch (and a 64 MB /tmp) are writable. The
# binary writes nothing but the two output folders and logs to stderr.
ENV DATA_DIR=/data OUTPUT_DIR=/output SCRATCH_DIR=/scratch HOME=/scratch TMPDIR=/scratch
USER 1000:1000
WORKDIR /app
CMD ["/app/shopping-sessions"]
