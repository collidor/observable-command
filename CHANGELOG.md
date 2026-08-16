## [1.0.4](https://github.com/collidor/observable-command/compare/v1.0.3...v1.0.4) (2026-08-16)


### Bug Fixes

* trigger patch release ([3a548c1](https://github.com/collidor/observable-command/commit/3a548c1e26304fd470945d916b84d0aba05f4754))

## [1.0.3](https://github.com/collidor/observable-command/compare/v1.0.2...v1.0.3) (2026-08-16)


### Bug Fixes

* **ci:** add dummy NPM_TOKEN to satisfy semantic-release preflight check for OIDC ([1f5efce](https://github.com/collidor/observable-command/commit/1f5efce25bbfa72a36c4627173dea838e98fa1a3))
* **ci:** decouple npm and jsr publish from semantic-release ([0ee0d71](https://github.com/collidor/observable-command/commit/0ee0d71aead6c49b7acf3caa18b25243fcc20018))
* **ci:** re-enable native semantic-release npm publishing for OIDC ([d00d485](https://github.com/collidor/observable-command/commit/d00d48598fbf7867cb6e4f90589c9acc0e1a4f9f))

## [1.0.2](https://github.com/collidor/observable-command/compare/v1.0.1...v1.0.2) (2026-08-15)


### Bug Fixes

* **ci:** add --allow-dirty to jsr publish to ignore uncommitted lockfiles ([b718644](https://github.com/collidor/observable-command/commit/b718644e408bcf6a385e3356c925d43a59a34eca))

## [1.0.1](https://github.com/collidor/observable-command/compare/v1.0.0...v1.0.1) (2026-08-15)


### Bug Fixes

* **ci:** rename workflow file to publish.yml to match npm trusted publisher configuration ([80248dc](https://github.com/collidor/observable-command/commit/80248dccc4ba9f57c3d9815aceba51773dab1f8e))

# 1.0.0 (2026-08-15)


### Bug Fixes

* **build:** add @swc/core devDependency required for tsup es5 build target ([10f9029](https://github.com/collidor/observable-command/commit/10f9029d9c4d64957fd0d86b5931352e948294c5))
* **ci:** configure npmPublish false and provenance publishCmd for tokenless npm OIDC, fix repo URL format ([81b2262](https://github.com/collidor/observable-command/commit/81b2262ef1fb8739c1a69a776959964e10378cb7))
* **ci:** update release pipeline to use OIDC trusted publishing without tokens ([0d372cd](https://github.com/collidor/observable-command/commit/0d372cd1041fe09a3090108fb4f2680b7bc7218c))


### Features

* add automated semantic-release pipeline for npm and jsr ([9d74dd3](https://github.com/collidor/observable-command/commit/9d74dd3ce5b21b41d57f01c6def282f7c8fb7f12))

# @collidor/observable-command

## 1.0.1

### Patch Changes

- fix imports
- Updated dependencies
  - @collidor/command@7.0.1

## 1.0.0

### Major Changes

- Y

### Patch Changes

- Updated dependencies
  - @collidor/command@7.0.0

## 0.0.3

### Patch Changes

- Add observable event
- Updated dependencies
  - @collidor/command@6.0.3

## 0.0.2

### Patch Changes

- Added observable command
- Updated dependencies
  - @collidor/command@6.0.2
