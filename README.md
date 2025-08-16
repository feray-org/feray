# feRay

[![image](https://img.shields.io/pypi/v/feray.svg)](https://pypi.python.org/pypi/feray)
[![image](https://img.shields.io/pypi/l/feray.svg)](https://pypi.python.org/pypi/feray)
[![image](https://img.shields.io/pypi/pyversions/feray.svg)](https://pypi.python.org/pypi/feray)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

---
Feature stores have unique needs.
This is an opinionated framework for building and managing feature stores on top of:

- [dagster](https://dagster.io/)
- [ray](https://ray.io/)
- [dagster-ray](https://github.com/danielgafni/dagster-ray)
- [delta](https://delta.io/) in particular [delta-rs](https://github.com/delta-io/delta-rs)
- [postgres](https://www.postgresql.org/)
- S3 compatible object stores

Core Concepts:

- enable rapid experimentation and iteraton with memoization
- a feature can have sub-features (feature containers)
- data and code version are tracked on the record level
- orchestration of distributed compute incl. bootstrap of environments


Further reading material

> TODO link docs here

- https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching
- https://gafni.dev/projects/sanas-ai-dagster-ray/
- https://dagster.io/blog/unlocking-flexible-pipelines-customizing-asset-decorator
- https://www.youtube.com/watch?v=HPqQSR0BoUQ
- https://georgheiler.com/post/paas-as-implementation-detail/
- https://www.samsara.com/blog/building-a-modern-machine-learning-platform-with-ray


## TODO - next steps

- Align on API
    - clarify recently added features
      - processing a single row
        - fastlane (serving)
        - rapid experimentation
        - but NOT a priority queue
      - feature flags/annotations
        - HITL verification
      - processing mode
        - batch (one ray cluster per feature)
        - fastlane (serving): one persistent ray cluster (at least for the task of the file e2e - perhaps long running even - lets start with per-file)
    - clarify the code design

- Align on skeleton setup

