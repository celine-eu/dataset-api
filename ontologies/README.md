# CELINE Ontology

This directory contains the **semantic artifacts** used in the CELINE project to support:

- semantic interoperability across datasets
- Digital Twins (WP3)
- Demonstrators, KPIs, and evaluation (WP5)
- mapping from tabular data to RDF / JSON-LD

The CELINE ontology is **not a standalone domain ontology**, but a **unified ontology profile** that aligns and connects established standards (SAREF, SOSA, BIGG, SEAS, EM-KPI) into a coherent semantic target for the CELINE ecosystem.

---

## Contents

### Core ontology artifacts

- **[CELINE JSON-LD context](celine.jsonld)**  
  JSON-LD `@context` defining prefixes, aliases, and mappings used by CELINE APIs and data pipelines.  
  This is the primary entry point for developers producing JSON-LD payloads.

- **[CELINE JSON Schema](celine.schema.json)**  
  JSON Schema used at API boundaries to validate incoming JSON-LD payloads before semantic expansion.

- **[CELINE ontology (Turtle)](celine.ttl)**  
  The formal OWL/RDF definition of the CELINE Unified Ontology Profile.  
  Defines CELINE classes and properties and aligns them with SAREF, SOSA, BIGG, SEAS, and EM-KPI.

- **[CELINE SHACL shapes](celine.shacl.ttl)**  
  SHACL shapes defining semantic constraints on the RDF graph after JSON-LD expansion.  
  Used to validate observations, time series, meters, energy communities, and KPIs.


---

### Repository & mapping configuration

- **[Open repository configuration](open-repository.yaml)**  
  YAML configuration listing datasets, governance metadata, and extension points for ontology mapping.

- **[Open repository JSON Schema](open-repository.schema.json)**  
  JSON Schema defining the structure of the dataset catalogue and repository configuration used by CELINE tooling.

---

## How these artifacts work together

The CELINE semantic stack follows a layered validation and mapping approach:

1. **Tabular data** is exposed via dataset APIs  
2. **Mapping definitions** bind dataset schemas to ontology classes and properties  
3. **JSON-LD** is generated using `celine.jsonld`  
4. **JSON Schema** (`celine.schema.json`) validates payload structure at the API level  
5. **JSON-LD expansion** produces RDF  
6. **SHACL validation** (`celine.shacl.ttl`) enforces semantic correctness  
7. Validated data is ingested into the **CELINE Digital Twin / Knowledge Graph**

This separation ensures:
- developer-friendly APIs
- strict semantic validation
- long-term interoperability

---

## Design principles

- **Standards first**: reuse ETSI SAREF, W3C SOSA/SSN, BIGG, SEAS, EM-KPI
- **Thin CELINE layer**: only project-specific glue concepts are defined
- **Modular & versionable**: artifacts can evolve independently
- **Tool-friendly**: compatible with rdflib, JSON-LD, SHACL engines

---

## Intended audience

- CELINE developers integrating data sources
- WP3 Digital Twin engineers
- WP5 demonstrator and KPI designers
- Data governance and interoperability stakeholders

---

## Versioning & publication

These ontology artifacts are published via **GitHub Pages** to provide stable, resolvable URLs suitable for:

- JSON-LD contexts
- ontology references in catalogues
- external integrations

Always prefer **versioned URLs** when referencing ontology artifacts in mappings or production systems.

---

## Questions & contributions

For questions, discussions, or proposed changes to the CELINE ontology profile, please refer to the main CELINE repository or open an issue in the relevant project repository.
