---
sidebar_position: 2
title: Tooling setup
---

You have to have uv installed for the library development - but pixi for executing the examples.

### Prerequisites

- **pixi installation**: https://pixi.sh/latest/installation/
  ```bash
  curl -fsSL https://pixi.sh/install.sh | sh
  ````
- install global tools
  ```bash
  pixi global install git make uv pre-commit
  ```

### Development setup

```bash
git clone https://github.com/feray-org.git
cd feray
pixi run pre-commit-install
pixi run pre-commit-run
```

### Useful commands during development

```bash
# fmt
pixi run -e build --frozen fmt

# lint
pixi run -e build --frozen lint

# test
pixi run -e build --frozen test

# documentation
pixi run -e docs --frozen docs-serve
pixi run -e docs --frozen docs-build
```

When committing - ensure semver compatible standard commit patterns are followed.

### Dependency upgrade

```bash
pixi update
pixi run -e build --frozen sync-lib-with-upgrade
cd examples pixi update
```