# sesam-py cleanup and efficiency plan

## Current baseline (from this repository)
- `sesam.py` is monolithic (~4307 LOC) and mixes CLI parsing, API calls, business rules, logging, and test orchestration.
- Largest functions are very long (`verify` ~300 LOC, `upload` ~182 LOC, `validate` ~154 LOC), which makes maintenance and testing harder.
- Test suite is heavily integration-driven (`test.sh` runs external-node tests) with very little isolated unit coverage (`tests/fair_weather_test/tests/test_pipe_id.py` only).
- Connector login modules use many broad `except Exception` blocks and mutable globals (notably in `connector_cli/oauth2login.py`).
- CI/tooling is partially modernized but inconsistent (older GitHub Actions versions, duplicated lint config, stale Travis badge in README).

## Quality criteria (definition of “cleaner and more efficient”)
Use these as acceptance criteria for refactoring work:

1. **Architecture & maintainability**
   - No single module >1500 LOC.
   - No function >60 LOC (except explicitly justified orchestration functions).
   - Cyclomatic complexity target: <10 per function (<15 for edge orchestration).
   - Clear layering: `cli` (argparse), `domain` (commands), `infra` (filesystem/http/sesamclient), `connectors`.

2. **Code quality & style**
   - Full compliance with `black + isort + flake8` in CI and pre-commit.
   - Replace broad exception handlers with explicit exception types.
   - Reduce mutable global state; prefer dependency injection and explicit function parameters.
   - Add type hints on all new/modified code and enforce with `mypy` (or `pyright`) incrementally.

3. **Testing**
   - Unit coverage floor: 70% overall, 85% for refactored modules.
   - Integration tests tagged and isolated from unit tests.
   - External-node tests run only in dedicated CI job with retries/timeouts; unit tests run on every PR quickly.
   - Critical flows covered: upload, validate, verify diffing, scheduler behavior, connector auth flows.

4. **Performance & runtime efficiency**
   - Define SLAs for high-cost commands (upload/test/verify).
   - Add lightweight timing instrumentation per command.
   - Benchmark before/after for refactors touching upload/verify/scheduler paths.
   - Minimize repeated JSON read/parse and redundant filesystem scans in loops.

5. **Reliability & security**
   - Timeouts and error handling on all outbound HTTP calls.
   - No process-kill side effects based on shell parsing (`lsof`/SIGKILL patterns should be replaced with controlled shutdown).
   - Secret-safe logging (never print tokens/credentials).
   - Dependency hygiene: pinned versions, periodic updates, vulnerability scanning in CI.

6. **Developer experience**
   - Single documented local workflow: setup, lint, unit, integration.
   - CI workflows use maintained GitHub Actions versions.
   - Documentation reflects current CI and release flow.

## Execution plan

### Phase 1 — Stabilize quality gates (quick wins, low risk)
- Update GitHub Actions to supported versions and consolidate lint/test workflow triggers.
- Remove stale Travis badge and align README with current CI/release reality.
- Keep one source of truth for lint config (prefer `pyproject.toml` + minimal `setup.cfg` fallback).
- Split test jobs:
  - `unit` job: pure local tests, no external token required.
  - `integration` job: existing external-node tests gated by secrets.

**Deliverables**
- Green CI with separated unit/integration jobs.
- Clear contribution doc with exact commands.

### Phase 2 — Extract modules from `sesam.py` (highest impact)
- Create package structure:
  - `sesam_cli/cli.py` (argument parsing and command dispatch)
  - `sesam_cli/commands/*.py` (upload/download/validate/verify/test/etc.)
  - `sesam_cli/node.py` (SesamNode/SesamCmdClient core)
  - `sesam_cli/io.py` / `sesam_cli/logging.py` helpers
- Supplementary step: enforce module boundaries so `sesam_cli/commands/` contains only true CLI command handlers, while non-command helpers are moved to dedicated namespaces (e.g. `sesam_cli/specs`, `sesam_cli/config`/`zip`, `sesam_cli/formatting`, `sesam_cli/connectors`).
- Refactor largest functions first (`verify`, `upload`, `validate`, scheduler logic) into smaller pure functions.
- Introduce typed data models (`dataclasses`) for command config/state.

**Deliverables**
- `sesam.py` reduced to thin entrypoint/compat shim.
- Refactored modules with unit tests for extracted pure logic.

### Phase 3 — Connector auth hardening and reuse
- Remove module-level mutable globals in `connector_cli/oauth2login.py`.
- Replace repeated “get secrets/get env/put env” patterns with shared helper service.
- Replace broad catches with explicit request/JSON/file exceptions and actionable messages.
- Add timeouts to all `requests` calls and centralize HTTP error translation.

**Deliverables**
- Deterministic, testable connector login flows.
- New unit tests for each auth variant error path and happy path.

### Phase 4 — Testing depth and speed
- Add offline unit tests for:
  - argument parsing and command routing
  - file diff/normalization logic
  - test spec loading and entity sorting
  - upload batching/rate controls
- Keep integration tests but tag them (e.g. `@pytest.mark.integration`) and run separately.
- Add coverage reporting and enforce threshold progressively.

**Deliverables**
- Fast PR feedback (<5 min unit job target).
- Confidence in core behavior without external node dependency.

### Phase 5 — Performance and observability
- Add per-command timing logs and optional `--profile` output.
- Benchmark large-config upload/verify scenarios.
- Optimize identified hotspots (JSON handling, directory scans, repeated API calls).

**Deliverables**
- Measured improvements with before/after numbers.

## Suggested work order (first 4 weeks)
1. Week 1: Phase 1 + baseline metrics (coverage, complexity, command timings).
2. Week 2: Start Phase 2 (extract CLI parser/dispatch + verify).
3. Week 3: Continue Phase 2 (upload/validate) + start Phase 3.
4. Week 4: Phase 4 unit-test expansion + integration split hardening.

## Tracking metrics (report weekly)
- LOC and max function length in main modules.
- Complexity distribution (functions >10 complexity).
- Unit/integration pass rates and durations.
- Coverage percentage (overall + critical modules).
- Number of broad exception handlers remaining.
