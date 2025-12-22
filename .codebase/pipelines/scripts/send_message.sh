#!/usr/bin/env bash

BOT_URL="https://open.larkoffice.com/open-apis/bot/v2/hook/8034ab53-6b8a-4794-aaf7-b194d6706774"
TEMPLATE_ID="AAqvdoOm73Z30"
TEMPLATE_VERSION="1.0.1"

function send_message() {
  local title="$1"
  local msg_body="$2"
  local color="${3}"

  payload="$(jq -n \
    --arg template_id "$TEMPLATE_ID" \
    --arg template_version "$TEMPLATE_VERSION" \
    --arg title "$title" \
    --arg msg_body "$msg_body" \
    --arg color "$color" \
    '{
      msg_type: "interactive",
      card: {
        type: "template",
        data: {
          template_id: $template_id,
          template_version_name: $template_version,
          template_variable: {
            title: $title,
            msg_body: $msg_body,
            color: $color
          }
        }
      }
    }'
  )"

  curl -X POST -H "Content-Type: application/json" -d "$payload" "$BOT_URL"

  echo "Message sent: $title"
}
