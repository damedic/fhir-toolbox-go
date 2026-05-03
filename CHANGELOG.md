# Changelog

## v0.0.3

### Bug Fixes
- Fix generic-only backends (e.g. REST client used as proxy) failing for R4 and R4B with "backend does not implement capabilities.ConcreteCapabilities" error

## v0.0.2 (v0.0.1 redacted because of repo rename)

### FHIR Model
- Generated FHIR R4, R4B, and R5 model types with JSON/XML (un)marshaling
- Build tags (`r4`, `r4b`, `r5`) for version-specific compilation
- Precision-preserving decimals using `apd.Decimal`
- Generated constants for required value sets
- Generated `MemSize` calculation for all elements

### REST Server
- Capability-driven REST server with automatic CapabilityStatement generation
- Concrete (type-safe) and generic (string-based) API with automatic wrapping and concrete method precedence
- CRUD, search with cursor-based pagination and `_include`
- FHIR Operations at system, type, and instance level with auto-discovery via reflection
- SearchParameter-based query parsing with optional strict validation
- JSON and XML response encoding (conditionally compiled)
- OperationOutcome error responses
- Multi-tenancy support via root-mounted handlers

### Client
- Typed REST client with generated resource-specific methods
- Generic client for arbitrary resource types
- Paginated search result iterator
- Configurable request format (JSON/XML)
- OperationOutcome error handling
- Operations support

### FHIRPath
- FHIRPath v2.0.0 compliant evaluation engine with broad v3.0.0 draft coverage
- ANTLR-based parser
- UCUM quantity conversions
- Configurable decimal precision via `apd.Context`
- Generated `TypeInfo` and `Children` for FHIR structs
- Configurable trace logging
