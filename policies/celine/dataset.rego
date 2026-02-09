# METADATA
# title: Dataset Authorization
# description: Specialized dataset policy with access_level enforcement
# scope: package
# entrypoint: true
package celine.dataset

import rego.v1

import data.celine.scopes

# =============================================================================
# DATASET AUTHORIZATION
# =============================================================================
#
# Specialized policy for datasets with access_level attribute:
#
#   open       - any authenticated service can query/read (no scope needed)
#   internal   - requires matching scope (default behavior)
#   restricted - requires dataset.admin scope only
#
# Scope mapping:
#   dataset.query  - execute queries
#   dataset.read   - read dataset metadata/schema
#   dataset.write  - modify dataset data
#   dataset.admin  - full access (create/delete, manage permissions)
#
# =============================================================================

default allow := false
default reason := "unauthorized"

# =============================================================================
# ACCESS LEVEL HELPERS
# =============================================================================

is_open if {
    input.resource.attributes.access_level == "open"
}

is_restricted if {
    input.resource.attributes.access_level == "restricted"
}

is_internal if {
    not is_open
    not is_restricted
}

# =============================================================================
# OPEN DATASETS
# =============================================================================

# Any authenticated service can query/read open datasets
allow if {
    scopes.is_service
    is_open
    input.action.name in ["query", "read"]
}

reason := "open dataset - public access" if {
    scopes.is_service
    is_open
    input.action.name in ["query", "read"]
}

# Write operations on open datasets still require scope
allow if {
    scopes.is_service
    is_open
    input.action.name in ["write", "create", "delete", "admin"]
    scopes.has_scope(required_scope)
}

reason := "open dataset - write authorized" if {
    scopes.is_service
    is_open
    input.action.name in ["write", "create", "delete", "admin"]
    scopes.has_scope(required_scope)
}

# =============================================================================
# RESTRICTED DATASETS
# =============================================================================

# Only dataset.admin can access restricted datasets
allow if {
    scopes.is_service
    is_restricted
    scopes.has_scope("dataset.admin")
}

reason := "restricted dataset - admin authorized" if {
    scopes.is_service
    is_restricted
    scopes.has_scope("dataset.admin")
}

# =============================================================================
# INTERNAL DATASETS (default)
# =============================================================================

# Standard scope-based access
allow if {
    scopes.is_service
    is_internal
    scopes.has_scope(required_scope)
}

reason := "dataset access authorized" if {
    scopes.is_service
    is_internal
    scopes.has_scope(required_scope)
}

# =============================================================================
# SCOPE DERIVATION
# =============================================================================

required_scope := "dataset.query" if {
    input.action.name == "query"
}

required_scope := "dataset.read" if {
    input.action.name == "read"
}

required_scope := "dataset.write" if {
    input.action.name == "write"
}

required_scope := "dataset.admin" if {
    input.action.name in ["create", "delete", "admin"]
}

# =============================================================================
# DENIAL REASONS
# =============================================================================

reason := "anonymous access denied" if {
    not allow
    scopes.is_anonymous
}

reason := "restricted dataset requires admin scope" if {
    not allow
    scopes.is_service
    is_restricted
}

reason := "missing dataset scope" if {
    not allow
    scopes.is_service
    is_internal
}

reason := "missing write scope for open dataset" if {
    not allow
    scopes.is_service
    is_open
    input.action.name in ["write", "create", "delete", "admin"]
}
