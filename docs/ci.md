# Continuous integration and package checks

The workflow at `.github/workflows/ci.yml` checks pushes, pull requests, and manual runs in GitHub Actions. This project's contents are the root of its standalone repository, so GitHub discovers and runs the workflow directly.

## What the workflow verifies

1. Set up Ubuntu 24.04, Python 3.12, and Temurin Java 21.
2. Install the exact dependency versions in `requirements.lock`, install the project, and verify dependency compatibility.
3. Lint source, tests, and release scripts; run the full test suite, including real local Spark transformations and transactional warehouse recovery.
4. Build a wheel with `pip wheel --no-deps`, then check required modules, bundled demo data, console entry point, archive paths, and exclusion of runtime state.
5. Create a separate virtual environment, install its locked dependencies and the wheel, and run from an empty temporary working directory with `PYTHONPATH` removed. Confirm the package is imported from that environment and the sample loads through `importlib.resources`; exercise CLI help and the complete offline demo.

The last step catches a common packaging failure: code works in a source checkout but depends on files that were never included in its distributable. The demo uses recorded observations and deterministic validation exercises, so the test needs no live NWS requests or API credentials. Java and dependency installation require network access on the hosted runner. Pipeline outputs stay in temporary directories, separate from any existing warehouse.

The workflow has read-only repository permissions, retains no checkout credentials, and has no deployment, package publishing, or messaging step. The action versions follow the official repositories: [checkout v7](https://github.com/actions/checkout), [setup-python v7](https://github.com/actions/setup-python), and [setup-java v6](https://github.com/actions/setup-java). These major-version references receive upstream updates; they are not immutable commit pins. Python libraries are pinned, while the Python and Java patch versions and build backend may update within their configured version constraints.

## Run the same checks locally

Use Python 3.12 and Java 21, then run from the project directory:

```bash
python -m pip install -r requirements.lock
python -m pip install --no-deps -e .
python -m pip check
python -m ruff check src tests scripts
python -m pytest -q
python -m pip wheel --no-deps --wheel-dir dist .
python scripts/check_release.py dist/weather_data_pipeline-*.whl
```

The wheel inspector prints a JSON result and exits nonzero on failure. Keep only the release wheel being checked in `dist` when using the wildcard. The workflow additionally performs the isolated installation and offline demo; copy that final workflow step to reproduce it on Linux, or use equivalent absolute paths on macOS. A local Spark run must be allowed to bind a loopback socket.

## Hosted activation and limitations

Confirm **Actions → Pipeline checks** completes successfully after changes are pushed. A passing local suite is evidence for the tested local environment, not evidence of a hosted run. Review the hosted result before making it a required branch check. Native Tableau rendering remains a separate manual check described in [dashboard validation](dashboard-validation.md); the CI runner verifies dashboard data and structure.
