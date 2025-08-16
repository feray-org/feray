# feRay
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

This is the raw library - independent of any specific orchestrator.

Further reading material

> TODO link docs here

- https://docs.dagster.io/guides/build/assets/asset-versioning-and-caching
- https://gafni.dev/projects/sanas-ai-dagster-ray/
- https://dagster.io/blog/unlocking-flexible-pipelines-customizing-asset-decorator
- https://www.youtube.com/watch?v=HPqQSR0BoUQ
- https://georgheiler.com/post/paas-as-implementation-detail/
- https://www.samsara.com/blog/building-a-modern-machine-learning-platform-with-ray
