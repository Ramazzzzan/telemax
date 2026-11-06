#!/usr/bin/env bash
# Deploy the checked-out Telemax release to the systemd production directory.
# Source: ~/distros/telemax; runtime state remains in ~/telemax.
set -Eeuo pipefail
umask 077

SOURCE_DIR="${TELEMAX_SOURCE_DIR:-$HOME/distros/telemax}"
PROD_DIR="${TELEMAX_PROD_DIR:-$HOME/telemax}"
SERVICE="${TELEMAX_SERVICE:-telemax.service}"
PIP_PROXY="${TELEMAX_PIP_PROXY:-socks5h://127.0.0.1:10808}"
REPO_SLUG='Ramazzzzan/telemax'
DRY_RUN=0
SKIP_CI=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --skip-ci-check) SKIP_CI=1 ;;
    *) printf 'Unknown argument: %s\n' "$arg" >&2; exit 2 ;;
  esac
done

say() { printf '[telemax-deploy] %s\n' "$*"; }
die() { printf '[telemax-deploy] ERROR: %s\n' "$*" >&2; exit 1; }
[[ $(id -u) -ne 0 ]] || die 'Run as htpc (not root); sudo is used only for systemctl.'
[[ -d "$SOURCE_DIR/.git" && -f "$SOURCE_DIR/telemax.py" && -f "$SOURCE_DIR/telemax_test.py" ]] || die "Not a Telemax git checkout: $SOURCE_DIR"
[[ -f "$PROD_DIR/constants.json" && -f "$PROD_DIR/telegram_queue.db" && -d "$PROD_DIR/venv" ]] || die "Missing existing production state in $PROD_DIR"
[[ -f "$SOURCE_DIR/deployment.md" && -f "$SOURCE_DIR/README.md" ]] || die 'Missing release documentation'
command -v python3 >/dev/null || die 'python3 is required'
command -v sqlite3 >/dev/null || die 'sqlite3 is required'
command -v curl >/dev/null || die 'curl is required'
command -v sudo >/dev/null || die 'sudo is required'

cd "$SOURCE_DIR"
[[ $(git branch --show-current) == main ]] || die 'Expected branch main'
git diff --quiet && git diff --cached --quiet || die 'Uncommitted tracked changes; commit or restore before deployment'
REV="$(git rev-parse HEAD)"
[[ "$REV" == "$(git rev-parse refs/remotes/origin/main)" ]] || die 'Local main differs from origin/main; run git pull --ff-only origin main'
VERSION="$(python3 telemax.py --version)"
SDK="$(python3 - <<'PY'
import ast
from pathlib import Path
m=ast.parse(Path('telemax.py').read_text(encoding='utf-8'))
for node in m.body:
    if isinstance(node, ast.Assign):
        if any(isinstance(t, ast.Name) and t.id == 'SDK_VERSION' for t in node.targets):
            print(ast.literal_eval(node.value))
            break
else:
    raise SystemExit('SDK_VERSION not found')
PY
)"
say "Source: $SOURCE_DIR at ${REV:0:12}, $VERSION, PyMax $SDK"
say "Production: $PROD_DIR; service: $SERVICE"
if (( DRY_RUN )); then
  say 'Dry run: no installation, service change, or file modification performed.'
  exit 0
fi

# Enforce successful push CI for this commit before doing anything to production.
# --skip-ci-check is an explicit emergency override; local tests still run.
if (( ! SKIP_CI )); then
  RUNS="$(curl --fail --silent --show-error --location --proxy "$PIP_PROXY" \
    --connect-timeout 15 --max-time 45 \
    -H 'Accept: application/vnd.github+json' \
    -H 'User-Agent: Telemax-deploy' \
    "https://api.github.com/repos/$REPO_SLUG/actions/runs?head_sha=$REV&per_page=30")" \
    || die 'Cannot check GitHub Actions; no deployment. Use --skip-ci-check only after verifying CI manually.'
  printf '%s' "$RUNS" | python3 -c '
import json,sys
j=json.load(sys.stdin)
runs=[r for r in j.get("workflow_runs",[]) if r.get("path")==".github/workflows/tests.yml" and r.get("event")=="push"]
if not runs or runs[0].get("status")!="completed" or runs[0].get("conclusion")!="success":
    print("GitHub Actions not green for this commit",file=sys.stderr)
    sys.exit(1)
' || die "GitHub Actions must be green for ${REV:0:12}; see https://github.com/$REPO_SLUG/actions"
  say 'GitHub Actions: success for this commit.'
else
  say 'WARNING: GitHub Actions check skipped by explicit request.'
fi

# Build and validate a completely separate venv before stopping the live service.
CANDIDATE="$(mktemp -d "$PROD_DIR/.venv-candidate-${REV:0:8}-XXXXXXXX")"
BACKUP=''
STOPPED=0
SWITCHED=0
SUCCESS=0
restore_on_error() {
  local rc=$?
  trap - EXIT
  if (( rc != 0 )); then
    say "Deployment failed (exit $rc)."
    if (( STOPPED )); then
      if (( SWITCHED )); then
        say 'Restoring previous code and venv (database was not replaced).'
        for f in telemax.py telemax_test.py deployment.md README.md; do
          if [[ -f "$BACKUP/$f" ]]; then cp -p "$BACKUP/$f" "$PROD_DIR/$f"; fi
        done
        if [[ -L "$PROD_DIR/venv" ]]; then rm "$PROD_DIR/venv"; fi
        if [[ -e "$BACKUP/venv" || -L "$BACKUP/venv" ]]; then mv "$BACKUP/venv" "$PROD_DIR/venv"; fi
      fi
      sudo systemctl start "$SERVICE" || say 'Old service restart failed; inspect systemctl/journalctl.'
    fi
    say 'Production database, constants and session_cache were never overwritten.'
  fi
  if (( ! SUCCESS )) && [[ -n "${CANDIDATE:-}" && -d "$CANDIDATE" ]]; then
    # Only cleanup the newly-built, unreferenced venv.
    if [[ ! -L "$PROD_DIR/venv" || "$(readlink "$PROD_DIR/venv")" != "$(basename "$CANDIDATE")" ]]; then
      rm -rf -- "$CANDIDATE"
    fi
  fi
  exit "$rc"
}
trap restore_on_error EXIT

python3 -m venv "$CANDIDATE"
# A fresh venv often lacks PySocks; bootstrap its pinned wheel through curl+SOCKS.
WHEEL="$CANDIDATE/PySocks-1.7.1-py3-none-any.whl"
curl --fail --location --silent --show-error --proxy "$PIP_PROXY" \
  --connect-timeout 15 --max-time 90 --retry 2 \
  'https://files.pythonhosted.org/packages/8d/59/b4572118e098ac8e46e399a1dd0f2d85403ce8bbaad9ec79373ed6badaf9/PySocks-1.7.1-py3-none-any.whl' -o "$WHEEL"
printf '%s  %s\n' '2725bd0a9925919b9b51739eea5f9e2bae91e83288108a9ad338b2e3a4435ee5' "$WHEEL" | sha256sum --check --status \
  || die 'PySocks wheel checksum failed'
"$CANDIDATE/bin/python" -m pip install --disable-pip-version-check --no-index "$WHEEL"
rm -f "$WHEEL"
"$CANDIDATE/bin/python" -m pip install --disable-pip-version-check --proxy "$PIP_PROXY" \
  --timeout 60 --retries 3 "maxapi-python==$SDK"
"$CANDIDATE/bin/python" -m pip check
"$CANDIDATE/bin/python" -m compileall -q telemax.py telemax_test.py
"$CANDIDATE/bin/python" -m unittest -q telemax_test
"$CANDIDATE/bin/python" telemax.py --config "$PROD_DIR/constants.json" --check-config
say 'Staging tests and config check: success.'

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP="$PROD_DIR/backups/deploy-${STAMP}-${REV:0:8}"
mkdir -p -m 700 "$BACKUP"
for f in telemax.py telemax_test.py deployment.md README.md; do
  if [[ -f "$PROD_DIR/$f" ]]; then cp -p "$PROD_DIR/$f" "$BACKUP/$f"; fi
done
say 'Stopping production and making SQLite backup.'
sudo systemctl stop "$SERVICE"
STOPPED=1
sqlite3 "$PROD_DIR/telegram_queue.db" ".backup '$BACKUP/telegram_queue.db'"
[[ "$(sqlite3 "$BACKUP/telegram_queue.db" 'PRAGMA quick_check;')" == ok ]] || die 'SQLite backup failed quick_check'

# Old venv stays intact as a rollback copy. Candidate remains at its original path.
SWITCHED=1
mv "$PROD_DIR/venv" "$BACKUP/venv"
ln -s "$(basename "$CANDIDATE")" "$PROD_DIR/venv"
for f in telemax.py telemax_test.py deployment.md README.md; do
  install -m 0644 "$SOURCE_DIR/$f" "$PROD_DIR/.$f.new"
  mv -f "$PROD_DIR/.$f.new" "$PROD_DIR/$f"
done
"$PROD_DIR/venv/bin/python" "$PROD_DIR/telemax.py" --check
[[ "$("$PROD_DIR/venv/bin/python" "$PROD_DIR/telemax.py" --version)" == "$VERSION" ]] || die 'Wrong installed version'
sudo systemctl start "$SERVICE"
sudo systemctl is-active --quiet "$SERVICE" || die 'Service failed to start'
STOPPED=0
SUCCESS=1
say "SUCCESS: $VERSION deployed; saved rollback in $BACKUP"
say "Service: $(sudo systemctl is-active "$SERVICE")"
