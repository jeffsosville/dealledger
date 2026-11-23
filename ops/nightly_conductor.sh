#!/bin/bash
# Runs up to MAX_JOBS (default 3) waiting Claude Code jobs unattended (headless), one after another.
# Queue: claude/staging/queue.txt — one job file path per line, top = next. Done jobs move to queue_done.txt.
# A job that ends without a summary stays in the queue; the runner moves on to the next one.
# Safety: Supabase keys are blanked, git commit/push are blocked, and all connectors (MCP) are off:
# unattended runs get local tools only (files, shell, proxy), so they cannot read live Supabase either.
# Output: claude/staging/<today>/SUMMARY_<job>.md + patches + SQL, for Jeff to review and push.
# Cron (Jeff's Mac): 30 8 * * * /bin/zsh -lc "$HOME/dealledger-repo/ops/nightly_conductor.sh"
cd "$(dirname "$0")/.." || exit 1
MAX_JOBS=${MAX_JOBS:-3}
mkdir -p logs claude/staging
Q=claude/staging/queue.txt; DONE=claude/staging/queue_done.txt
touch "$Q" "$DONE"
LOG="logs/conductor_$(date +%Y%m%d).log"
TODAY=$(date +%Y-%m-%d); mkdir -p "claude/staging/$TODAY"

# One conductor at a time: runs share the proxy and the queue. A lock whose process is gone is stale.
LOCK=logs/conductor.lock
if ! mkdir "$LOCK" 2>/dev/null; then
  OLD=$(cat "$LOCK/pid" 2>/dev/null)
  if [ -n "$OLD" ] && kill -0 "$OLD" 2>/dev/null; then
    echo "$(date) conductor already running (pid $OLD); this start exits" >> "$LOG"; exit 0
  fi
  echo "$(date) clearing stale conductor lock (pid ${OLD:-none})" >> "$LOG"
  rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null
  mkdir "$LOCK" 2>/dev/null || { echo "$(date) could not take conductor lock" >> "$LOG"; exit 1; }
fi
echo $$ > "$LOCK/pid"
trap 'rm -f "$LOCK/pid"; rmdir "$LOCK" 2>/dev/null' EXIT

set -a; source "$HOME/.dealledger.env"; set +a
export SUPABASE_SERVICE_KEY= SUPABASE_SERVICE_ROLE_KEY= SUPABASE_KEY=
unset ANTHROPIC_API_KEY
# cron's PATH is minimal: look in the usual install spots too.
export PATH="$HOME/.local/bin:$HOME/.claude/local:/opt/homebrew/bin:/usr/local/bin:$HOME/.npm-global/bin:$PATH"
# claude -p stops background workers 10 min after the conductor's last turn; let them finish.
export CLAUDE_CODE_PRINT_BG_WAIT_CEILING_MS=0
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
    --permission-mode bypassPermissions --strict-mcp-config \
    --disallowedTools "Bash(git commit:*)" "Bash(git push:*)" "Bash(git reset:*)" "Bash(git checkout:*)" \
      "mcp__claude_ai_Supabase" RemoteTrigger PushNotification CronCreate CronDelete DesignSync WebSearch \
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
