IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
FIXTURES ?= fixtures
ANSWER ?= dev-answer
PY ?= python3
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev native native-fixtures native-dev unit cross-check

## Build the image for the scorer's platform
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	$(PY) scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare the dev output with the published dev answer (dev-answer.zip unzipped here)
check-dev:
	$(PY) scripts/compare.py out $(ANSWER)

## Without Docker: build the binary with cargo
native:
	cargo build --release

unit:
	cargo test --release

native-fixtures: native
	$(PY) scripts/run_fixtures.py $(FIXTURES) -- target/release/shopping-sessions

native-dev: native
	rm -rf out && time target/release/shopping-sessions $(DEV) out

## Rust vs the slow Python reference on freshly generated datasets
cross-check: native
	PY=$(PY) scripts/cross_check.sh target/release/shopping-sessions 2:150000 3:300000 4:450000 5:150000 9:300000
