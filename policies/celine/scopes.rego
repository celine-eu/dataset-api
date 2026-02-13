# METADATA
# title: Scope Matching
# description: Shared scope matching utilities
# scope: package
package celine.scopes

import rego.v1

# =============================================================================
# SCOPE MATCHING
# =============================================================================
#
# Scope convention: {service}.{action}
#
# Matching rules:
#   1. Exact match: "dataset.query" matches "dataset.query"
#   2. Admin override: "dataset.admin" matches any "dataset.*"
#
# =============================================================================

# Check if subject has required scope
has_scope(required) if {
    some have in input.subject.scopes
    scope_matches(have, required)
}

# Check if subject has any of the required scopes
has_any_scope(required_list) if {
    some required in required_list
    has_scope(required)
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
# SUBJECT HELPERS
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
