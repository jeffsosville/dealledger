#!/bin/bash
# Runs ONE waiting Claude Code job unattended (headless), then stops.
# Queue: claude/staging/queue.txt — one job file path per line, top = next. Done jobs move to queue_done.txt.
# Safety: Supabase keys are blanked (no DB writes possible), git commit/push are blocked.
# Output: claude/staging/<today>/SUMMARY_<job>.md + patches + SQL, for Jeff to review and push.
# Cron (Jeff's Mac): 30 8 * * * /bin/zsh -lc "$HOME/dealledger-repo/ops/nightly_conductor.sh"
cd "$(dirname "$0")/.." || exit 1
REPO="$(pwd)"
mkdir -p logs claude/staging
Q=claude/staging/queue.txt; DONE=claude/staging/queue_done.txt
touch "$Q" "$DONE"
JOB=$(grep -v '^\s*#' "$Q" | grep -v '^\s*$' | head -1)
LOG="logs/conductor_$(date +%Y%m%d).log"
[ -z "$JOB" ] && { echo "$(date) queue empty" >> "$LOG"; exit 0; }
[ -f "$JOB" ] || { echo "$(date) job file missing: $JOB" >> "$LOG"; exit 1; }
TODAY=$(date +%Y-%m-%d); mkdir -p "claude/staging/$TODAY"
NAME=$(basename "$JOB" .md)
SUMMARY="claude/staging/$TODAY/SUMMARY_${NAME}.md"

set -a; source "$HOME/.dealledger.env"; set +a
export SUPABASE_SERVICE_KEY= SUPABASE_SERVICE_ROLE_KEY= SUPABASE_KEY=
unset ANTHROPIC_API_KEY
CLAUDE=$(command -v claude)
[ -z "$CLAUDE" ] && { echo "$(date) claude not on PATH" >> "$LOG"; exit 1; }

PROMPT="$(cat ops/conductor-prompt.md)

UNATTENDED RUN (no human present). Job file: $JOB — read it and do that job only.
Do not wait for approval: deliver everything into claude/staging/$TODAY/ and finish by
writing $SUMMARY (what changed, per-broker results, how to apply: the exact bash, the SQL
files in order with rollbacks, and the change_watch rows). Never commit or push. Never
write to Supabase (keys are blanked on purpose). If something needs Jeff, say so at the
top of the summary under 'NEEDS JEFF' and stop that part."

echo "=== conductor run $(date) job=$JOB ===" >> "$LOG"
caffeinate -i "$CLAUDE" -p "$PROMPT" --model opus \
  --permission-mode bypassPermissions \
  --disallowedTools "Bash(git commit:*)" "Bash(git push:*)" "Bash(git reset:*)" "Bash(git checkout:*)" \
  >> "$LOG" 2>&1
RC=$?
echo "=== exit $RC $(date) ===" >> "$LOG"
if [ -f "$SUMMARY" ]; then
  grep -vxF "$JOB" "$Q" > "$Q.tmp" && mv "$Q.tmp" "$Q"
  echo "$TODAY $JOB -> $SUMMARY" >> "$DONE"
else
  echo "no summary written; job stays at top of queue" >> "$LOG"
fi
