# CHANGELOG

<!-- version list -->

## v1.7.0 (2026-04-27)

### Chores

- Upgrade celine-sdk to 1.12.0
  ([`bd2850b`](https://github.com/celine-eu/dataset-api/commit/bd2850b8dde913680bfcf4ceb3f93f017a48daee))

### Features

- Add governance override logic
  ([`3967fd7`](https://github.com/celine-eu/dataset-api/commit/3967fd7540d36e93b9422d666480f7514973a2d1))


## v1.6.4 (2026-04-22)

### Bug Fixes

- Add ST_Transform
  ([`82714be`](https://github.com/celine-eu/dataset-api/commit/82714be7586bac81815095c7a773983d29866edb))


## v1.6.3 (2026-04-22)

### Bug Fixes

- Add skip_count option, allow more st_* SQL functions
  ([`9452731`](https://github.com/celine-eu/dataset-api/commit/945273187b3f01e099226c90fc0844453719a969))

### Chores

- Upgrade celine-sdk to 1.11.0
  ([`bb360cb`](https://github.com/celine-eu/dataset-api/commit/bb360cbc345a4185168415fc55d17c3b1a97aff6))


## v1.6.2 (2026-04-21)

### Bug Fixes

- Increase default timeout
  ([`bdd4e8c`](https://github.com/celine-eu/dataset-api/commit/bdd4e8c3d6541cdbcfee794e0e98be9687a13a5e))


## v1.6.1 (2026-04-21)

### Bug Fixes

- Add configurable query timeout
  ([`2663d17`](https://github.com/celine-eu/dataset-api/commit/2663d177ea7c1b51cb7c0d76cf2e745db57de186))

### Chores

- Add comments to onwers
  ([`275bef6`](https://github.com/celine-eu/dataset-api/commit/275bef65d053a963a7b4f504d2af1927b3264a90))


## v1.6.0 (2026-04-16)

### Bug Fixes

- Allow scoped client to bypass row filter
  ([`e5065d9`](https://github.com/celine-eu/dataset-api/commit/e5065d9394caf7a7d0e09207289c7e6f409b1111))

- Allow tautological queries (1=1)
  ([`d853cb5`](https://github.com/celine-eu/dataset-api/commit/d853cb5f8f2a040ba5b83e4db0d24a7d73a69659))

### Chores

- Update owners
  ([`3919a63`](https://github.com/celine-eu/dataset-api/commit/3919a632fc47854e72344a9cb65f57ed55a2258b))

- Update sample owners
  ([`1d9fa2b`](https://github.com/celine-eu/dataset-api/commit/1d9fa2bc214f406f570192924f3bf27673f717ce))

- Upgrade celine-sdk to 1.10.0
  ([`0126265`](https://github.com/celine-eu/dataset-api/commit/0126265749e1f284fb9af1d6d228b75a21843190))

- Upgrade celine-sdk to 1.6.0
  ([`cfa8c07`](https://github.com/celine-eu/dataset-api/commit/cfa8c074e4b31d239ac09e7170874effbdb39a93))

- Upgrade celine-sdk to 1.7.0
  ([`8df28dd`](https://github.com/celine-eu/dataset-api/commit/8df28dd779b98f8ee7dd88b601b9776072f6f9c9))

- Upgrade celine-sdk to 1.8.0
  ([`ec210d3`](https://github.com/celine-eu/dataset-api/commit/ec210d31d1553f446bbc2a7b124dd6a61805c8f5))

- Upgrade celine-sdk to 1.9.0
  ([`1469aac`](https://github.com/celine-eu/dataset-api/commit/1469aac31c56c23b66430580ee8910b70eb7d585))

### Features

- Allow more sql function
  ([`47b6bd9`](https://github.com/celine-eu/dataset-api/commit/47b6bd95b299706fd5070b6612df924446f8d45f))


## v1.5.0 (2026-04-03)

### Bug Fixes

- Uniform expose flag, add owners.yaml support, review DataspaceConfig
  ([`657ef78`](https://github.com/celine-eu/dataset-api/commit/657ef78a4e22032579008a9366eceb5da3698fdd))

### Chores

- Upgrade celine-sdk to 1.5.0
  ([`15dd734`](https://github.com/celine-eu/dataset-api/commit/15dd734de758718e248bccf403fa1ba3bc7757e2))

- **deps**: Bump the runtime-dependencies group across 1 directory with 4 updates
  ([`528e42e`](https://github.com/celine-eu/dataset-api/commit/528e42ee4cac3a3a80c3244506e93739fc4a6e04))

- **deps-dev**: Bump the development-dependencies group across 1 directory with 3 updates
  ([`16db176`](https://github.com/celine-eu/dataset-api/commit/16db1766a6afc7089c363246cfe6930506c765d0))

### Features

- Add compliant dssc catalogue, add EDC key handling
  ([`e484727`](https://github.com/celine-eu/dataset-api/commit/e48472720c08de9ef8920c69ccc8c6c41ce2c528))

- Refactor owners. Add time function to SQL AST
  ([`a017a3b`](https://github.com/celine-eu/dataset-api/commit/a017a3b774b87962d38b56550434178d7aea5cf8))


## v1.4.0 (2026-03-23)

### Chores

- Add gov import/export cli
  ([`f2776a1`](https://github.com/celine-eu/dataset-api/commit/f2776a1ffd3fb41a21f37eae72f4afa395425297))

- Upgrade celine-sdk to 1.4.3
  ([`17e6d8e`](https://github.com/celine-eu/dataset-api/commit/17e6d8eaa7258ad8bf8204a2137b9c3c98dfd6b7))

### Features

- Add catalogue load directly from governance.yaml
  ([`f5e8fe5`](https://github.com/celine-eu/dataset-api/commit/f5e8fe536c64ccfeb296684b5c32ef0ffa39bbde))


## v1.3.4 (2026-03-06)

### Bug Fixes

- Review token passing in row filter
  ([`05bec51`](https://github.com/celine-eu/dataset-api/commit/05bec510f47393dcb5b3a3feda414169cfe71a9a))


## v1.3.3 (2026-03-06)

### Bug Fixes

- Policies allowed reason label is misleading
  ([`de44943`](https://github.com/celine-eu/dataset-api/commit/de449435476d0d837fa49eae34faab5dfa2f77ce))


## v1.3.2 (2026-03-06)

### Bug Fixes

- Handle 3 parts ds reference
  ([`cdfaf74`](https://github.com/celine-eu/dataset-api/commit/cdfaf74d60c5c11b0dbd34a222044d273530f440))


## v1.3.1 (2026-03-06)

### Bug Fixes

- Improve ds normalization
  ([`ca84159`](https://github.com/celine-eu/dataset-api/commit/ca84159c31c4d4f1ea502dc4e08e57e4daf0bc3b))


## v1.3.0 (2026-03-06)

### Features

- Split catalogue vs datasets connection
  ([`f75a452`](https://github.com/celine-eu/dataset-api/commit/f75a452f9652ac114f09c8e08afe0becf90e4679))


## v1.2.0 (2026-03-06)

### Features

- Add row filter handling via CLI
  ([`20fad6c`](https://github.com/celine-eu/dataset-api/commit/20fad6cfb8f18c6cb45d64b416dcad0778b8b2fc))


## v1.1.3 (2026-03-03)

### Bug Fixes

- Drop duplicates
  ([`c988d1b`](https://github.com/celine-eu/dataset-api/commit/c988d1bb76d75196b640297ac396050ad853807a))


## v1.1.2 (2026-03-03)

### Bug Fixes

- Bump
  ([`31cdd6b`](https://github.com/celine-eu/dataset-api/commit/31cdd6b26af39ffc1c12278a210138029713ef07))


## v1.1.1 (2026-03-02)

### Bug Fixes

- Compose healthcheck
  ([`78682bc`](https://github.com/celine-eu/dataset-api/commit/78682bc307bb7692d21034b27868fa7b9bb8631a))

- Update docker setup
  ([`9806fc6`](https://github.com/celine-eu/dataset-api/commit/9806fc604bbbd60933004008a47606d4f23cda0a))


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
