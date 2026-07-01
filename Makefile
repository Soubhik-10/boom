RPC ?=
URL ?=
ENGINE_RPC ?=
JWT ?= ./jwt.hex
OUT ?= runs/local-001
CONFIG ?=
GENERATED_CONFIG ?= runs/generated-basic-eth.toml
DURATION ?= 30s
CONCURRENCY ?= 64
RPS ?=
RAMP ?=
BATCH_SIZE ?= 1
SCENARIO ?=
RUSTDOCFLAGS ?= -D warnings
export RUSTDOCFLAGS

POSITIONAL := $(word 2,$(MAKECMDGOALS))
ifeq ($(firstword $(MAKECMDGOALS)),probe)
ifneq ($(POSITIONAL),)
RPC := $(POSITIONAL)
endif
endif
ifeq ($(firstword $(MAKECMDGOALS)),bench)
ifneq ($(POSITIONAL),)
RPC := $(POSITIONAL)
endif
endif
ifneq ($(URL),)
RPC := $(URL)
endif

ifeq ($(strip $(CONFIG)),)
BENCH_ARGS := $(RPC) --eth --duration $(DURATION) --concurrency $(CONCURRENCY) --batch-size $(BATCH_SIZE) --out $(OUT) $(if $(RPS),--rps $(RPS),) $(if $(RAMP),--ramp $(RAMP),) $(if $(SCENARIO),--scenario $(SCENARIO),)
else
BENCH_ARGS := --config $(CONFIG) --out $(OUT)
endif

.PHONY: help ci lint fmt fmt-fix clippy clippy-fix test check docs build install run probe catalog metrics live ws-bench find-limit bench bench-all bench-heavy bench-rate bench-ramp bench-compare bench-basic engine report report-print report-open report-prompt gen-config engine-ssz-suite engine-ssz-capabilities engine-ssz-identity engine-ssz-bodies-range

help:
	@echo "boom targets:"
	@echo "  make install     - install the boom binary with cargo install"
	@echo "  make ci          - lint and test"
	@echo "  make lint        - nightly fmt + nightly clippy"
	@echo "  make probe RPC=http://localhost:8545"
	@echo "  make run RPC=http://localhost:8545"
	@echo "  make catalog RPC=http://localhost:8545"
	@echo "  make live RPC=http://localhost:8545 OUT=runs/live"
	@echo "  make ws-bench WS=ws://localhost:8546 OUT=runs/ws"
	@echo "  make find-limit RPC=http://localhost:8545 OUT=runs/limit"
	@echo "  make probe http://localhost:8545"
	@echo "  make bench RPC=http://localhost:8545 OUT=runs/local-001"
	@echo "  make bench-all RPC=http://localhost:8545 OUT=runs/all"
	@echo "  make bench-heavy RPC=http://localhost:8545 DURATION=120s CONCURRENCY=512 OUT=runs/heavy"
	@echo "  make bench-rate RPC=http://localhost:8545 RPS=500 OUT=runs/rate"
	@echo "  make bench-ramp RPC=http://localhost:8545 RAMP=100:1000 OUT=runs/ramp"
	@echo "  make bench-compare LEFT=http://localhost:8545 RIGHT=http://localhost:9545 OUT=runs/compare"
	@echo "  make report-print OUT=runs/heavy"
	@echo "  make report-open OUT=runs/heavy"
	@echo "  make report-prompt OUT=runs/heavy"
	@echo "  make metrics OUT=runs/heavy"
	@echo "  make bench CONFIG=configs/examples/basic-eth.toml OUT=runs/local-001"
	@echo "  make engine ENGINE_RPC=http://localhost:8551 JWT=./jwt.hex"
	@echo "  make engine-ssz-suite ENGINE_RPC=http://localhost:8551 JWT=./jwt.hex"
	@echo "  make engine-ssz-capabilities ENGINE_RPC=http://localhost:8551 JWT=./jwt.hex"

ci: lint test docs

lint: fmt clippy

fmt:
	cargo +nightly fmt --all -- --check

fmt-fix:
	cargo +nightly fmt --all

clippy:
	cargo +nightly clippy --workspace --lib --examples --tests --benches --all-features --locked -- -D warnings

clippy-fix:
	cargo +nightly clippy --workspace --lib --examples --tests --benches --all-features --locked --fix --allow-dirty --allow-staged -- -D warnings

test:
	cargo test --workspace --all-targets --locked

check:
	cargo check --workspace --all-targets --locked

docs:
	cargo +nightly doc --workspace --no-deps --locked

build:
	cargo build --release --locked

install:
	cargo install --path crates/cli --bin boom --locked --force

probe:
	@test -n "$(RPC)" || { echo "RPC required. Usage: make probe RPC=http://localhost:8545 or make probe http://localhost:8545"; exit 1; }
	cargo run -q --locked -- probe --rpc $(RPC)

run:
	cargo run -q --locked -- run $(RPC)

catalog:
	@test -n "$(RPC)" || { echo "RPC required. Usage: make catalog RPC=http://localhost:8545"; exit 1; }
	cargo run -q --locked -- catalog --rpc $(RPC) --all

metrics:
	cargo run -q --locked -- metrics --run $(OUT) --print

live:
	@test -n "$(RPC)" || { echo "RPC required. Usage: make live RPC=http://localhost:8545"; exit 1; }
	cargo run -q --locked -- live $(RPC) --all --duration $(DURATION) --concurrency $(CONCURRENCY) --out $(OUT)

ws-bench:
	@test -n "$(WS)" || { echo "WS required. Usage: make ws-bench WS=ws://localhost:8546"; exit 1; }
	cargo run -q --locked -- ws-bench $(WS) --duration $(DURATION) --concurrency $(CONCURRENCY) --out $(OUT)

find-limit:
	@test -n "$(RPC)" || { echo "RPC required. Usage: make find-limit RPC=http://localhost:8545"; exit 1; }
	cargo run -q --locked -- find-limit $(RPC) --out $(OUT) --duration $(DURATION) --concurrency $(CONCURRENCY) --scenario $(if $(SCENARIO),$(SCENARIO),explorer)

gen-config:
	@test -n "$(RPC)" || { echo "RPC required when CONFIG is not set. Usage: make bench RPC=http://localhost:8545 or make bench CONFIG=..."; exit 1; }
	@mkdir -p $(dir $(GENERATED_CONFIG))
	@sed \
		-e 's|rpc = "http://localhost:8545"|rpc = "$(RPC)"|' \
		-e 's|duration = "30s"|duration = "$(DURATION)"|' \
		-e 's|concurrency = 64|concurrency = $(CONCURRENCY)|' \
		configs/examples/basic-eth.toml > $(GENERATED_CONFIG)

bench:
ifeq ($(strip $(CONFIG)),)
	@test -n "$(RPC)" || { echo "RPC required. Usage: make bench RPC=http://localhost:8545 or make bench CONFIG=..."; exit 1; }
endif
	cargo run -q --locked -- bench $(BENCH_ARGS)

bench-all:
	@test -n "$(RPC)" || { echo "RPC required. Usage: make bench-all RPC=http://localhost:8545"; exit 1; }
	cargo run -q --locked -- bench $(RPC) --all --duration $(DURATION) --concurrency $(CONCURRENCY) --out $(OUT)

bench-heavy:
	@test -n "$(RPC)" || { echo "RPC required. Usage: make bench-heavy RPC=http://localhost:8545 DURATION=120s CONCURRENCY=512"; exit 1; }
	cargo run -q --locked -- bench $(RPC) --all --duration $(DURATION) --concurrency $(CONCURRENCY) --batch-size $(BATCH_SIZE) --out $(OUT)

bench-rate:
	@test -n "$(RPC)" || { echo "RPC required. Usage: make bench-rate RPC=http://localhost:8545 RPS=500"; exit 1; }
	@test -n "$(RPS)" || { echo "RPS required. Usage: make bench-rate RPC=http://localhost:8545 RPS=500"; exit 1; }
	cargo run -q --locked -- bench $(RPC) --all --duration $(DURATION) --concurrency $(CONCURRENCY) --batch-size $(BATCH_SIZE) --rps $(RPS) --out $(OUT)

bench-ramp:
	@test -n "$(RPC)" || { echo "RPC required. Usage: make bench-ramp RPC=http://localhost:8545 RAMP=100:1000"; exit 1; }
	@test -n "$(RAMP)" || { echo "RAMP required. Usage: make bench-ramp RPC=http://localhost:8545 RAMP=100:1000"; exit 1; }
	cargo run -q --locked -- bench $(RPC) --all --duration $(DURATION) --concurrency $(CONCURRENCY) --batch-size $(BATCH_SIZE) --ramp $(RAMP) --out $(OUT)

bench-compare:
	@test -n "$(LEFT)" || { echo "LEFT required. Usage: make bench-compare LEFT=http://localhost:8545 RIGHT=http://localhost:9545"; exit 1; }
	@test -n "$(RIGHT)" || { echo "RIGHT required. Usage: make bench-compare LEFT=http://localhost:8545 RIGHT=http://localhost:9545"; exit 1; }
	cargo run -q --locked -- compare $(LEFT) $(RIGHT) --all --duration $(DURATION) --concurrency $(CONCURRENCY) --batch-size $(BATCH_SIZE) --out $(OUT)

bench-basic:
	cargo run -q --locked -- bench --config configs/examples/basic-eth.toml --out $(OUT)

engine:
	@test -n "$(ENGINE_RPC)" || { echo "ENGINE_RPC required. Usage: make engine ENGINE_RPC=http://localhost:8551 JWT=./jwt.hex"; exit 1; }
	cargo run -q --locked -- engine --rpc $(ENGINE_RPC) --jwt $(JWT)

engine-ssz-suite:
	@test -n "$(ENGINE_RPC)" || { echo "ENGINE_RPC required"; exit 1; }
	cargo run -q --locked -- engine-ssz-suite --base $(ENGINE_RPC) --jwt $(JWT) --accept application/json

engine-ssz-capabilities:
	@test -n "$(ENGINE_RPC)" || { echo "ENGINE_RPC required"; exit 1; }
	cargo run -q --locked -- engine-ssz --base $(ENGINE_RPC) --jwt $(JWT) --endpoint capabilities --accept application/json

engine-ssz-identity:
	@test -n "$(ENGINE_RPC)" || { echo "ENGINE_RPC required"; exit 1; }
	cargo run -q --locked -- engine-ssz --base $(ENGINE_RPC) --jwt $(JWT) --endpoint identity --accept application/json

engine-ssz-bodies-range:
	@test -n "$(ENGINE_RPC)" || { echo "ENGINE_RPC required"; exit 1; }
	cargo run -q --locked -- engine-ssz --base $(ENGINE_RPC) --jwt $(JWT) --endpoint bodies-by-range --fork prague

report:
	cargo run -q --locked -- report --run $(OUT)

report-print:
	cargo run -q --locked -- report --run $(OUT) --print

report-open:
	cargo run -q --locked -- report --run $(OUT) --open

report-prompt:
	cargo run -q --locked -- report --run $(OUT) --prompt

%:
	@:




