package s3

import rego.v1

# ---------------------------------------------------------------------------
# Input contract (sent by s3sentinel per request):
#
#   input.principal  — JWT `sub` claim (ZITADEL user ID or email for human users)
#   input.email      — JWT `email` claim (present for human users, empty for machine users)
#   input.groups     — JWT `groups` claim ([]string)
#   input.action     — S3 action: "GetObject" | "PutObject" | "DeleteObject" | ...
#   input.bucket     — bucket name
#   input.key        — object key (empty string for bucket-level operations)
#
# Data contract (loaded from policies/data.json via kube-mgmt):
#
#   data.policies             — named IAM-style policy documents
#   data.principal_policies   — maps principal/email/group → list of policy names
# ---------------------------------------------------------------------------

# Default deny — explicit Allow required.
default allow := false

# Allow when at least one statement grants access and none explicitly deny.
allow if {
    not explicitly_denied
    explicitly_allowed
}

# ---------------------------------------------------------------------------
# Resolve principal: map ZITADEL user ID → product name via principals.json.
# Falls back to the raw principal if no mapping exists (e.g. human users).
# ---------------------------------------------------------------------------

resolved_principal := name if {
    name := data.principal_names[input.principal]
} else := input.principal

# ---------------------------------------------------------------------------
# Build the resource ARN for the current request
# ---------------------------------------------------------------------------

request_resource := sprintf("arn:s3sentinel:s3:::%s/%s", [input.bucket, input.key]) if {
    input.key != ""
}

request_resource := sprintf("arn:s3sentinel:s3:::%s", [input.bucket]) if {
    input.key == ""
}

# ---------------------------------------------------------------------------
# Map simplified policy actions ("read"/"write") to S3 action sets
# ---------------------------------------------------------------------------

read_actions := {
    "s3:GetObject", "s3:HeadObject",
    "s3:ListObjects", "s3:ListObjectsV2"
}

write_actions := read_actions | {
    "s3:PutObject", "s3:DeleteObject", "s3:HeadBucket",
    "s3:CreateMultipartUpload", "s3:UploadPart",
    "s3:CompleteMultipartUpload", "s3:AbortMultipartUpload"
}

# Normalise action to AWS style (s3:GetObject) for matching
request_action := sprintf("s3:%s", [input.action])

# ---------------------------------------------------------------------------
# Check whether any applicable statement explicitly denies the request
# ---------------------------------------------------------------------------

explicitly_denied if {
    some policy_name in data.principal_policies[resolved_principal]
    _stmt_denies(data.policies[policy_name])
}

explicitly_denied if {
    input.email != ""
    some policy_name in data.principal_policies[input.email]
    _stmt_denies(data.policies[policy_name])
}

explicitly_denied if {
    some group in input.groups
    some policy_name in data.principal_policies[group]
    _stmt_denies(data.policies[policy_name])
}

# ---------------------------------------------------------------------------
# Check whether any applicable statement explicitly allows the request
# ---------------------------------------------------------------------------

explicitly_allowed if {
    some policy_name in data.principal_policies[resolved_principal]
    _stmt_allows(data.policies[policy_name])
}

explicitly_allowed if {
    input.email != ""
    some policy_name in data.principal_policies[input.email]
    _stmt_allows(data.policies[policy_name])
}

explicitly_allowed if {
    some group in input.groups
    some policy_name in data.principal_policies[group]
    _stmt_allows(data.policies[policy_name])
}

_stmt_denies(policy) if {
    some stmt in policy.Statement
    stmt.Effect == "Deny"
    action_matches(stmt.Action, request_action)
    resource_matches(stmt.Resource, request_resource)
}

_stmt_allows(policy) if {
    some stmt in policy.Statement
    stmt.Effect == "Allow"
    action_matches(stmt.Action, request_action)
    resource_matches(stmt.Resource, request_resource)
}

# ---------------------------------------------------------------------------
# Action matching — supports wildcards (s3:*, s3:Get*)
# ---------------------------------------------------------------------------

action_matches(actions, action) if {
    is_string(actions)
    _action_match(actions, action)
}

action_matches(actions, action) if {
    is_array(actions)
    some a in actions
    _action_match(a, action)
}

_action_match(pattern, _)      if { pattern == "*" }
_action_match(pattern, action) if { pattern == action }
_action_match(pattern, action) if {
    endswith(pattern, "*")
    prefix := trim_suffix(pattern, "*")
    startswith(action, prefix)
}
_action_match("read", action)  if { action in read_actions }
_action_match("write", action) if { action in write_actions }

# ---------------------------------------------------------------------------
# Resource matching — supports ARN wildcards (arn:s3sentinel:s3:::bucket/prefix/*)
# ---------------------------------------------------------------------------

resource_matches(resources, resource) if {
    is_string(resources)
    _resource_match(resources, resource)
}

resource_matches(resources, resource) if {
    is_array(resources)
    some r in resources
    _resource_match(r, resource)
}

_resource_match(pattern, _)        if { pattern == "*" }
_resource_match(pattern, resource) if { pattern == resource }
_resource_match(pattern, resource) if {
    endswith(pattern, "*")
    prefix := trim_suffix(pattern, "*")
    startswith(resource, prefix)
}
