#!/usr/bin/env bash
# API helpers for the isolated adjacent-release compatibility fixture.
# The caller provides service_url, test_root, admin_token, and admin_password.

api_request() {
  local service="$1"
  local method="$2"
  local path="$3"
  local payload="${4:-}"
  local etag="${5:-}"
  local approval="${6:-}"
  local allow_not_found="${7:-false}"
  local url
  local body_file="$test_root/api-body.json"
  local headers_file="$test_root/api-headers.txt"
  local args=(
    --silent --show-error
    --request "$method"
    --output "$body_file"
    --dump-header "$headers_file"
    --write-out '%{http_code}'
    --header 'Accept: application/json'
  )

  url="$(service_url "$service")"
  if [[ -n "$admin_token" ]]; then
    args+=(--header "Authorization: Bearer $admin_token")
  fi
  if [[ -n "$payload" ]]; then
    args+=(--header 'Content-Type: application/json' --data "$payload")
  fi
  if [[ -n "$etag" ]]; then
    args+=(--header "If-Match: $etag")
  fi
  if [[ -n "$approval" ]]; then
    args+=(--header "X-Hubuum-Credential-Approval: $approval")
  fi

  api_status="$(curl "${args[@]}" "$url$path")" || return
  api_body="$(cat "$body_file")"
  api_headers="$(cat "$headers_file")"
  if [[ ! "$api_status" =~ ^2[0-9][0-9]$ ]]; then
    if [[ "$allow_not_found" == true && "$api_status" == 404 ]]; then
      return 0
    fi
    echo "ERROR: $service $method $path returned HTTP $api_status" >&2
    printf '%s\n' "$api_body" >&2
    return 1
  fi
}

credential_request() {
  local service="$1"
  local path="$2"
  local payload="$3"
  local operation="$4"
  local approval_request
  local approval=""
  local expires_at

  approval_request="$(jq --null-input --arg password "$admin_password" \
    --argjson operation "$operation" '{password: $password, operation: $operation}')" || return
  api_request "$service" POST /api/v1/iam/credential-approvals \
    "$approval_request" "" "" true || return
  case "$api_status" in
    201)
      approval="$(jq --exit-status --raw-output \
        '.approval | strings | select(length > 0)' <<< "$api_body")" || return
      if [[ "$(jq --raw-output '.kind' <<< "$operation")" == create_token ]]; then
        expires_at="$(jq --exit-status --raw-output \
          '.token_expires_at | strings | select(length > 0)' <<< "$api_body")" || return
        payload="$(jq --arg expires_at "$expires_at" \
          '.expires_at = $expires_at' <<< "$payload")" || return
      fi
      ;;
    404)
      # Releases before credential approvals retain the legacy request flow.
      ;;
    *)
      echo "ERROR: unexpected credential approval response HTTP $api_status" >&2
      return 1
      ;;
  esac
  api_request "$service" POST "$path" "$payload" "" "$approval"
}

create_user() {
  local service="$1"
  local payload="$2"
  local operation
  operation="$(jq --null-input --argjson user "$payload" \
    '{kind: "create_user", user: $user}')" || return
  credential_request "$service" /api/v1/iam/users "$payload" "$operation"
}

create_principal_token() {
  local service="$1"
  local principal_id="$2"
  local payload="$3"
  local operation
  operation="$(jq --null-input --argjson principal_id "$principal_id" \
    --argjson token "$payload" \
    '{kind: "create_token", principal_id: $principal_id, token: $token}')" || return
  credential_request "$service" "/api/v1/iam/principals/$principal_id/tokens" \
    "$payload" "$operation"
}
