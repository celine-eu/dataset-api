# METADATA
# title: Dataset Authorization
# description: Specialized dataset policy with access_level enforcement
# scope: package
# entrypoint: true
package celine.dataset

import rego.v1

# =============================================================================
# DATASET AUTHORIZATION
# =============================================================================
#
# Subjects:
#   - service: client credentials with scopes (dataset.query, dataset.admin, etc.)
#   - user: human users with groups (admins, managers, etc.)
#
# Access levels:
#   - open: any authenticated subject can read
#   - internal: services need scope, users need group membership
#   - restricted: requires dataset.admin scope or admins group
#
# =============================================================================

default allow := false
default reason := "unauthorized"

# =============================================================================
# SUBJECT TYPE HELPERS (inline, no import alias)
# =============================================================================

is_service if {
    input.subject.type == "service"
}

is_user if {
    input.subject.type == "user"
}

is_anonymous if {
    input.subject == null
}

is_anonymous if {
    input.subject.type == "anonymous"
}

# =============================================================================
# SCOPE HELPERS (inline, no import alias)
# =============================================================================

has_scope(required) if {
    some have in input.subject.scopes
    scope_matches(have, required)
}

# Exact match
scope_matches(have, want) if {
    have == want
}

# Admin override: {service}.admin matches {service}.*
scope_matches(have, want) if {
    endswith(have, ".admin")
    service := trim_suffix(have, ".admin")
    startswith(want, concat("", [service, "."]))
}

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
# REQUIRED SCOPE (single function, no conflicts)
# =============================================================================

get_required_scope(action) := "dataset.query" if {
    action in ["query", "read"]
}

get_required_scope(action) := "dataset.write" if {
    action == "write"
}

get_required_scope(action) := "dataset.admin" if {
    action in ["create", "delete", "admin"]
}

# =============================================================================
# OPEN DATASETS
# =============================================================================

# Any authenticated subject can read open datasets
allow if {
    is_open
    input.action.name in ["query", "read"]
    not is_anonymous
}

reason := "open dataset access granted" if {
    allow
    is_open
    input.action.name in ["query", "read"]
}

# Write on open datasets requires scope
allow if {
    is_open
    is_service
    input.action.name in ["write", "create", "delete", "admin"]
    has_scope(get_required_scope(input.action.name))
}

# =============================================================================
# RESTRICTED DATASETS
# =============================================================================

# Services need dataset.admin
allow if {
    is_restricted
    is_service
    has_scope("dataset.admin")
}

reason := "restricted dataset - admin scope granted" if {
    allow
    is_restricted
    is_service
}

# Users need admins group
allow if {
    is_restricted
    is_user
    "admins" in input.subject.groups
}

reason := "restricted dataset - admin group granted" if {
    allow
    is_restricted
    is_user
}

# =============================================================================
# INTERNAL DATASETS
# =============================================================================

# Services need matching scope
allow if {
    is_internal
    is_service
    has_scope(get_required_scope(input.action.name))
}

reason := "internal dataset - scope granted" if {
    allow
    is_internal
    is_service
}

# Users with admins group get full access
allow if {
    is_internal
    is_user
    "admins" in input.subject.groups
}

reason := "internal dataset - admin group granted" if {
    allow
    is_internal
    is_user
    "admins" in input.subject.groups
}

# Users with managers group can read
allow if {
    is_internal
    is_user
    "managers" in input.subject.groups
    input.action.name in ["query", "read"]
}

allow if {
    is_internal
    is_user
    "viewers" in input.subject.groups
    input.action.name in ["query", "read"]
}

reason := "internal dataset - manager group granted" if {
    allow
    is_internal
    is_user
    "managers" in input.subject.groups
}

# =============================================================================
# DENIAL REASONS
# =============================================================================

reason := "anonymous access denied" if {
    not allow
    is_anonymous
}

reason := "restricted dataset requires admin scope or group" if {
    not allow
    is_restricted
}

reason := "missing required scope" if {
    not allow
    is_service
    is_internal
}

reason := "user not in authorized group" if {
    not allow
    is_user
    is_internal
}
