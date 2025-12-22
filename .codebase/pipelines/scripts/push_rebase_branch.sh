#!/usr/bin/env bash
set -exuo pipefail
# Push rebase-main into main after CI check successfully.

REMOTE=origin
SRC=rebase-main
DST=main
COMMIT_URL_PREFIX="https://git.byted.org/dp/bolt/commit"
SUBMODULE_PATH=bytedance_internal
SUBMODULE_URL_PREFIX="https://code.byted.org/dp/bolt-internal/commit"

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"/send_message.sh

git fetch "$REMOTE"
git checkout "$SRC"
git submodule update --init --recursive

# Commits on main with no patch-equivalent in rebase-main
lost=$(git range-diff origin/main...rebase-main | \
       grep -E '^[[:space:]]*[0-9]+:[[:space:]]+[0-9a-f]+[[:space:]]+<[[:space:]]+-:' || true)

if [ -n "$lost" ]; then
  echo "ERROR: mising commits during rebase:"
  echo "$lost"
  exit 1
fi

# Superproject: commits added by rebase-main on top of main
extra_shas=$(git cherry "$REMOTE/$DST" "$SRC" | awk '$1=="+" {print $2}')
[[ -n "$extra_shas" ]] || { echo "$SRC has no new commits over $REMOTE/$DST"; exit 0; }

commit_count=$(echo "$extra_shas" | wc -l | tr -d ' ')
main_log=$(git log --no-walk --pretty=format:"- [\`%h\`]($COMMIT_URL_PREFIX/%H) %s" $extra_shas)

# Submodule: linear ancestor relationship, use A..B directly.
old=$(git ls-tree "$REMOTE/$DST" -- "$SUBMODULE_PATH" | awk '{print $3}')
new=$(git ls-tree "$SRC"          -- "$SUBMODULE_PATH" | awk '{print $3}')
sm_log=""
sm_count=0
if [[ "$old" != "$new" ]]; then
    git -C "$SUBMODULE_PATH" fetch --quiet origin
    sm_log=$(git -C "$SUBMODULE_PATH" log \
        --pretty=format:"- [\`%h\`]($SUBMODULE_URL_PREFIX/%H) %s" "$old..$new")
    sm_count=$(git -C "$SUBMODULE_PATH" rev-list --count "$old..$new")
fi

git push --force-with-lease "$REMOTE" "$SRC:$DST"

echo "========================================"
echo "$SRC adds $commit_count commit(s) over $REMOTE/$DST:"
echo "$main_log"
if [[ "$sm_count" -gt 0 ]]; then
    echo
    echo "Submodule $SUBMODULE_PATH adds $sm_count commit(s):"
    echo "$sm_log"
fi
echo "========================================"

body="**bolt** ($commit_count commits):
$main_log"
if [[ "$sm_count" -gt 0 ]]; then
    body+=$'\n\n'"**$SUBMODULE_PATH** ($sm_count commits):
$sm_log"
fi
send_message "Successfully promoted $SRC to $DST" "$body" "green"
