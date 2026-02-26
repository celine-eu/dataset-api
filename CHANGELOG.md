# CHANGELOG

<!-- version list -->

## v1.1.0 (2026-02-26)

### Bug Fixes

- Add extra exp, improve regexp for multi statement check, use is_service_client from sdk
  ([`fc0d4c1`](https://github.com/celine-eu/dataset-api/commit/fc0d4c176ae64145d2e072a9e1a84281c816467e))

- Include templates in package
  ([`4024be1`](https://github.com/celine-eu/dataset-api/commit/4024be125cd093766687c0f3c840ec813a7c616f))

- Refactor, review api
  ([`31078f5`](https://github.com/celine-eu/dataset-api/commit/31078f5f33af728d1fb46b29da38265044c62778))

- Use regex pattern to determine XSD content for files without file extension
  ([`3d16b8c`](https://github.com/celine-eu/dataset-api/commit/3d16b8c70465771d65c088960c2a89cb157d9103))

- Use resolved URL for determining file name (useful in case of redirects)
  ([`03a3308`](https://github.com/celine-eu/dataset-api/commit/03a330821ce93981a50422b127319ea7cb72d281))

### Chores

- Add debugger, logging
  ([`d38ddc3`](https://github.com/celine-eu/dataset-api/commit/d38ddc3b5e69a84eb17baf3231be5c7bfd616ccf))

- Add dumpster
  ([`e644b8a`](https://github.com/celine-eu/dataset-api/commit/e644b8a16bac6b65f829a6b3ef4f0e7d5b8dddb5))

- Add pkg permissions
  ([`1dc4c8b`](https://github.com/celine-eu/dataset-api/commit/1dc4c8bf2a48079d848acbbd6c51cf6415d43d17))

- Cleanup
  ([`f6dce97`](https://github.com/celine-eu/dataset-api/commit/f6dce97337ac242ecd840c792168819bd216df1c))

- Fix docker build, add alembic, add compose deps
  ([`af9a3a7`](https://github.com/celine-eu/dataset-api/commit/af9a3a7492cd41ed3e343d8c7a4d8720686bc3a8))

- Move ontologies to dedicated repo https://github.com/celine-eu/ontologies
  ([`7653259`](https://github.com/celine-eu/dataset-api/commit/76532596ffbeca5bffacce39dab8ace3c5d33b9f))

- Move to src/
  ([`a5004fc`](https://github.com/celine-eu/dataset-api/commit/a5004fcb4823675670a8bc96e32f7a616dc17ea3))

- Review packaging and docker setup
  ([`2738fd0`](https://github.com/celine-eu/dataset-api/commit/2738fd0a585a661580312d8093ab1cb87b6e90e8))

- Run uvcorn from venv
  ([`8c2db9a`](https://github.com/celine-eu/dataset-api/commit/8c2db9a34d1f1b40be968e254d5c681cb668cf33))

- Update taskfile
  ([`75feb15`](https://github.com/celine-eu/dataset-api/commit/75feb15a01100776bd02ab3baadab325af4e2a50))

- Upgrade celine-sdk
  ([`29d1d66`](https://github.com/celine-eu/dataset-api/commit/29d1d66511736ae8f3d60b0dfd74e3da6109f3c2))

- Upgrade celine-sdk
  ([`154b29e`](https://github.com/celine-eu/dataset-api/commit/154b29ed5bcd86e455e7f757796d63f422fcb1d9))

- Upgrade taskfile with setup
  ([`dffbe29`](https://github.com/celine-eu/dataset-api/commit/dffbe290687e568a4d467f4bea1b4b1d4bfe92da))

- Versioned ontology, review modelling to adopt PECO,SAREF,BIGG. added Scenario and Snapshot for
  simulation model.
  ([`df0afb9`](https://github.com/celine-eu/dataset-api/commit/df0afb976eda6c5978448d4684fea74dd318d6d0))

- **deps**: Bump alembic from 1.17.2 to 1.18.1
  ([`9a65328`](https://github.com/celine-eu/dataset-api/commit/9a65328b289d4a42f4923e84433ea4b37b520296))

- **deps**: Bump fastapi from 0.124.0 to 0.128.0
  ([`b241253`](https://github.com/celine-eu/dataset-api/commit/b24125344108ccec4a7ed7fef60882ceb2032e27))

- **deps**: Bump sqlglot from 28.1.0 to 28.3.0
  ([`ffd180e`](https://github.com/celine-eu/dataset-api/commit/ffd180e3ca59268a05a5c793f5ec7a8986b367d3))

- **deps**: Bump sqlglot from 28.3.0 to 28.6.0
  ([`535b19e`](https://github.com/celine-eu/dataset-api/commit/535b19e03b45e89cc7ed73492a08108b2b00bca2))

- **deps**: Bump the runtime-dependencies group across 1 directory with 2 updates
  ([`4c5d953`](https://github.com/celine-eu/dataset-api/commit/4c5d953536689b44e17eb5a80486aada7849d52c))

- **deps**: Bump typer from 0.20.0 to 0.21.1
  ([`dee2c70`](https://github.com/celine-eu/dataset-api/commit/dee2c70e6a18bbe215947257ec687fac544de970))

- **deps-dev**: Bump hypothesis in the development-dependencies group
  ([`f016112`](https://github.com/celine-eu/dataset-api/commit/f01611289ada22d84c2a071aaa4942f850c8354a))

- **deps-dev**: Bump the development-dependencies group across 1 directory with 2 updates
  ([`024fabe`](https://github.com/celine-eu/dataset-api/commit/024fabe58ab17151673ac7bf14dfdbb30c995abf))

### Continuous Integration

- Bump the actions group with 2 updates
  ([`5bceda3`](https://github.com/celine-eu/dataset-api/commit/5bceda3450142e17b92b5f7e2d437b2b8b358e92))

### Features

- Add full query parsing support
  ([`3befd61`](https://github.com/celine-eu/dataset-api/commit/3befd61357bb6a382a6972f518bbd873ee40723b))

- Add html views
  ([`d27df3d`](https://github.com/celine-eu/dataset-api/commit/d27df3dfd6bf2c95c218d4cfabebf5b9b698d099))

- Add postgres export, integate policies
  ([`28e6780`](https://github.com/celine-eu/dataset-api/commit/28e678030399ceaeb54c8ef8af3657971fdaca4b))

- Add row filters engine
  ([`7131140`](https://github.com/celine-eu/dataset-api/commit/7131140af3b37c49d553a1829e5d8acce5d875f5))

- Add user column filter
  ([`b3dae7e`](https://github.com/celine-eu/dataset-api/commit/b3dae7e672f6e46d597c37083f60b807c6f68c59))

- Check for table and remove unexistant in catalogue import
  ([`f8f4819`](https://github.com/celine-eu/dataset-api/commit/f8f48199da1a87b2cec526670ca42cecfca5a9a0))

- Integrate policy
  ([`e44c7c3`](https://github.com/celine-eu/dataset-api/commit/e44c7c3f2937b1d2e48d32eebcd5580776c79290))

- Migrate policies to repo, integate local client
  ([`e29e821`](https://github.com/celine-eu/dataset-api/commit/e29e8213ae86e50650944305de36c122c008bf3e))

- More SQL testing coverage (injection, fuzzy)
  ([`a0ebe46`](https://github.com/celine-eu/dataset-api/commit/a0ebe46bdeef979306ee884ef5072b50c3aed462))

- Refactor, drop ast traversing, use query validation
  ([`4cfeb9f`](https://github.com/celine-eu/dataset-api/commit/4cfeb9f6a8ef6a3bf02247696f0002fa93a5f4e6))

- Revire OPA call
  ([`2288910`](https://github.com/celine-eu/dataset-api/commit/2288910dfe6e8e0af62114ba45dbb69807f64cd4))


## v1.0.0 (2025-12-17)

- Initial Release
