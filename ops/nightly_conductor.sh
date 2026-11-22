#!/bin/bash
# Runs up to MAX_JOBS (default 3) waiting Claude Code jobs unattended (headless), one after another.
# Queue: claude/staging/queue.txt — one job file path per line, top = next. Done jobs move to queue_done.txt.
# A job that ends without a summary stays in the queue; the runner moves on to the next one.
# Safety: Supabase keys are blanked (no DB writes possible), git commit/push are blocked.
# Output: claude/staging/<today>/SUMMARY_<job>.md + patches + SQL, for Jeff to review and push.
# Cron (Jeff's Mac): 30 8 * * * /bin/zsh -lc "$HOME/dealledger-repo/ops/nightly_conductor.sh"
cd "$(dirname "$0")/.." || exit 1
MAX_JOBS=${MAX_JOBS:-3}
mkdir -p logs claude/staging
Q=claude/staging/queue.txt; DONE=claude/staging/queue_done.txt
touch "$Q" "$DONE"
LOG="logs/conductor_$(date +%Y%m%d).log"
TODAY=$(date +%Y-%m-%d); mkdir -p "claude/staging/$TODAY"

set -a; source "$HOME/.dealledger.env"; set +a
export SUPABASE_SERVICE_KEY= SUPABASE_SERVICE_ROLE_KEY= SUPABASE_KEY=
unset ANTHROPIC_API_KEY
CLAUDE=$(command -v claude)
[ -z "$CLAUDE" ] && { echo "$(date) claude not on PATH" >> "$LOG"; exit 1; }

# Snapshot the jobs for this run so queue edits mid-run don't shift things.
JOBS=$(grep -v '^\s*#' "$Q" | grep -v '^\s*$' | head -n "$MAX_JOBS")
[ -z "$JOBS" ] && { echo "$(date) queue empty" >> "$LOG"; exit 0; }

while IFS= read -r JOB; do
  [ -f "$JOB" ] || { echo "$(date) job file missing: $JOB" >> "$LOG"; continue; }
  NAME=$(basename "$JOB" .md)
  SUMMARY="claude/staging/$TODAY/SUMMARY_${NAME}.md"

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
    >> "$LOG" 2>&1 < /dev/null
  RC=$?
  echo "=== exit $RC $(date) job=$JOB ===" >> "$LOG"
  if [ -f "$SUMMARY" ]; then
    grep -vxF "$JOB" "$Q" > "$Q.tmp" && mv "$Q.tmp" "$Q"
    echo "$TODAY $JOB -> $SUMMARY" >> "$DONE"
  else
    echo "no summary written for $JOB; it stays in the queue" >> "$LOG"
  fi
done <<< "$JOBS"
