#!/usr/bin/env bash
# post-task-lint.sh
#
# Stop hook — runs after Claude finishes a task.
# Lints only the source files Claude edited this session.
# If lint fails, outputs {"decision":"block","reason":"..."} to feed
# errors back to Claude so it fixes them before finishing.

set -uo pipefail

# ── Read hook context ─────────────────────────────────────────────────────────
INPUT=$(cat)

SESSION_ID=$(echo "$INPUT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('session_id', 'default'))
" 2>/dev/null || echo "default")

# Prevent re-entrant execution within the same stop event
STOP_HOOK_ACTIVE=$(echo "$INPUT" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('stop_hook_active', False))
" 2>/dev/null || echo "False")

[[ "$STOP_HOOK_ACTIVE" == "True" ]] && exit 0

# ── Gather changed files ──────────────────────────────────────────────────────
QUEUE_FILE="/tmp/.claude_lint_${SESSION_ID}"
declare -a RAW_FILES=()

if [[ -f "$QUEUE_FILE" ]]; then
  while IFS= read -r f; do
    [[ -n "$f" ]] && RAW_FILES+=("$f")
  done < "$QUEUE_FILE"
  rm -f "$QUEUE_FILE"
fi

# Fallback: git diff for modified tracked files + new untracked source files
if [[ ${#RAW_FILES[@]} -eq 0 ]]; then
  while IFS= read -r f; do
    [[ -n "$f" ]] && RAW_FILES+=("$f")
  done < <(git diff --name-only 2>/dev/null || true)
  while IFS= read -r f; do
    [[ -n "$f" ]] && RAW_FILES+=("$f")
  done < <(git ls-files --others --exclude-standard 2>/dev/null \
    | grep -E '\.(ts|js|py|rb|go)$' || true)
fi

# ── Filter files ──────────────────────────────────────────────────────────────
SKIP_PATTERNS=("docs/" ".yaml" ".yml" ".md" ".json" ".lock" "fixtures/" "__fixtures__" "dist/" ".spec." ".test.")

UNIQUE=()

for file in "${RAW_FILES[@]+"${RAW_FILES[@]}"}"; do
  [[ -z "$file" ]] && continue

  # Make path relative to project root
  rel="${file#$(pwd)/}"

  # Skip if already seen (linear scan — fine for small lists)
  already_seen=false
  for u in "${UNIQUE[@]+"${UNIQUE[@]}"}"; do
    [[ "$u" == "$rel" ]] && already_seen=true && break
  done
  $already_seen && continue

  # Skip if matches any skip pattern
  skip=false
  for pattern in "${SKIP_PATTERNS[@]}"; do
    [[ "$rel" == *"$pattern"* ]] && skip=true && break
  done
  $skip && continue

  # Skip if file no longer exists
  [[ ! -f "$rel" ]] && continue

  UNIQUE+=("$rel")
done

[[ ${#UNIQUE[@]} -eq 0 ]] && exit 0

# Enforce max-file guard (full project lint if over 50)
SCOPE_FILES=()
if [[ ${#UNIQUE[@]} -le 50 ]]; then
  SCOPE_FILES=("${UNIQUE[@]+"${UNIQUE[@]}"}")
fi

# ── Detect linter ─────────────────────────────────────────────────────────────
LINTER=""

if [[ -f "package.json" ]] && python3 -c "
import json, sys
s = json.load(open('package.json')).get('scripts', {})
sys.exit(0 if 'lint' in s else 1)
" 2>/dev/null; then
  LINTER="npm"
elif [[ -f "eslint.config.mjs" || -f "eslint.config.js" || -f ".eslintrc.js" || -f ".eslintrc.json" || -f ".eslintrc.yml" ]]; then
  LINTER="eslint"
elif [[ -f "pyproject.toml" ]] && grep -q '\[tool\.ruff\]' pyproject.toml 2>/dev/null; then
  LINTER="ruff"
elif [[ -f "setup.cfg" ]] && grep -q '\[flake8\]' setup.cfg 2>/dev/null; then
  LINTER="flake8"
elif [[ -f ".rubocop.yml" ]]; then
  LINTER="rubocop"
elif [[ -f "go.mod" ]]; then
  LINTER="golangci-lint"
else
  echo "⚠️  No linter detected — skipping lint check."
  exit 0
fi

# ── Build scoped lint command ─────────────────────────────────────────────────
declare -a LINT_CMD=()

case "$LINTER" in
  npm)
    # npm run lint uses eslint — call eslint directly to support file scoping
    if [[ ${#SCOPE_FILES[@]} -gt 0 ]]; then
      LINT_CMD=("npx" "eslint" "--fix" "${SCOPE_FILES[@]+"${SCOPE_FILES[@]}"}")
    else
      LINT_CMD=("npm" "run" "lint" "--" "--fix")
    fi
    ;;
  eslint)
    if [[ ${#SCOPE_FILES[@]} -gt 0 ]]; then
      LINT_CMD=("npx" "eslint" "--fix" "${SCOPE_FILES[@]+"${SCOPE_FILES[@]}"}")
    else
      LINT_CMD=("npx" "eslint" "--fix" ".")
    fi
    ;;
  ruff)
    if [[ ${#SCOPE_FILES[@]} -gt 0 ]]; then
      LINT_CMD=("ruff" "check" "${SCOPE_FILES[@]+"${SCOPE_FILES[@]}"}")
    else
      LINT_CMD=("ruff" "check" ".")
    fi
    ;;
  flake8)
    if [[ ${#SCOPE_FILES[@]} -gt 0 ]]; then
      LINT_CMD=("flake8" "${SCOPE_FILES[@]+"${SCOPE_FILES[@]}"}")
    else
      LINT_CMD=("flake8" ".")
    fi
    ;;
  rubocop)
    if [[ ${#SCOPE_FILES[@]} -gt 0 ]]; then
      LINT_CMD=("rubocop" "${SCOPE_FILES[@]+"${SCOPE_FILES[@]}"}")
    else
      LINT_CMD=("rubocop")
    fi
    ;;
  golangci-lint)
    LINT_CMD=("golangci-lint" "run")
    ;;
esac

# ── Run linter ────────────────────────────────────────────────────────────────
LINT_OUTPUT_FILE=$(mktemp)
"${LINT_CMD[@]}" > "$LINT_OUTPUT_FILE" 2>&1
LINT_EXIT=$?

if [[ $LINT_EXIT -eq 0 ]]; then
  rm -f "$LINT_OUTPUT_FILE"
  echo "✅ Lint passed"
  exit 0
fi

# ── Lint failed — block and feed errors back to Claude ────────────────────────
FILES_LIST=$(printf '%s\n' "${UNIQUE[@]+"${UNIQUE[@]}"}")

python3 - "$LINT_OUTPUT_FILE" <<EOF
import json, sys

lint_output = open(sys.argv[1]).read()
files_list = """$FILES_LIST"""

reason = (
    "Linter failed. Fix all errors before completing the task.\n\n"
    "Files checked:\n"
    + "\n".join(f"  {f}" for f in files_list.strip().splitlines())
    + "\n\nLint output:\n"
    + lint_output
)

print(json.dumps({"decision": "block", "reason": reason}))
EOF

rm -f "$LINT_OUTPUT_FILE"
exit 1
