# Shopping Sessions: one static native Rust binary, built with
# profile-guided optimization inside this build (scripts/build.sh).
#
# TARGET_CPU (default cascadelake) is fixed at build time. The scorers run
# GCP n2 (Intel Cascade Lake or Ice Lake, both AVX-512); a cascadelake build
# runs on either, where `native` built on an Ice Lake host could use
# instructions a Cascade Lake runner lacks. Override with
#   docker build --build-arg TARGET_CPU=x86-64-v3 .   (any x86-64 with AVX2)
FROM rust:1-slim AS build
ARG TARGET_CPU=cascadelake
# llvm-profdata, to merge the PGO profile.
RUN rustup component add llvm-tools
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tools ./tools
COPY fixtures ./fixtures
COPY scripts/build.sh ./scripts/build.sh
RUN mkdir -p /out && TARGET_CPU="$TARGET_CPU" sh scripts/build.sh /out/shopping-sessions

# The binary is static (no loader, no shared libraries at start), so the
# base image costs nothing at run time. It is a small OS rather than
# `scratch` because scripts/docker_run.sh runs `chmod` inside the image.
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
