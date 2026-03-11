#!/usr/bin/env bash
# track-changes.sh
#
# PostToolUse hook — triggered on every Edit and Write tool call.
# Appends the edited file path to a session-scoped queue file so the
# Stop hook can lint only the files Claude actually touched.

INPUT=$(cat)

SESSION_ID=$(echo "$INPUT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('session_id', 'default'))
" 2>/dev/null || echo "default")

FILE_PATH=$(echo "$INPUT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('tool_input', {}).get('file_path', ''))
" 2>/dev/null || true)

[[ -z "$FILE_PATH" ]] && exit 0

echo "$FILE_PATH" >> "/tmp/.claude_lint_${SESSION_ID}"
exit 0
