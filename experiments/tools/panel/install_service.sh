#!/usr/bin/env bash
# Installs the experiment panel as a systemd service that starts at boot (and restarts if it dies).
#
#   experiments/tools/panel/install_service.sh                 # install + start, for the calling user
#   sudo experiments/tools/panel/install_service.sh --user entall   # as root (CloudLab startup)
#   experiments/tools/panel/install_service.sh --remove        # stop + uninstall
#
# The panel runs as the user that runs the experiments, never as root: the runners call sudo
# themselves. Safe to run again (it reinstalls and restarts; running jobs are not affected).
set -euo pipefail
NAME=experiment-panel
UNIT=/etc/systemd/system/$NAME.service
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)
user="" remove=0
while [ $# -gt 0 ]; do
  case "$1" in
    --remove) remove=1 ;;
    --user) user=${2:?--user needs a name}; shift ;;
    --user=*) user=${1#*=} ;;
    -h|--help) sed -n '2,9p' "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
  shift
done
SUDO=sudo; [ "$(id -u)" = 0 ] && SUDO=""

if [ "$remove" = 1 ]; then
  $SUDO systemctl disable --now "$NAME" 2>/dev/null || true
  $SUDO rm -f "$UNIT"
  $SUDO systemctl daemon-reload
  echo "removed $NAME"
  exit 0
fi

user=${user:-${SUDO_USER:-$(id -un)}}
[ "$user" = root ] && { echo "the panel must not run as root: pass --user <experiment user>" >&2; exit 1; }
id "$user" > /dev/null 2>&1 || { echo "no such user: $user" >&2; exit 1; }
home=$(getent passwd "$user" | cut -d: -f6)
[ -d "$home" ] || { echo "home of $user ($home) does not exist" >&2; exit 1; }
if [ "$(id -u)" = 0 ]; then
  sudo -u "$user" sudo -n true 2>/dev/null || { echo "$user needs passwordless sudo (the runners use it)" >&2; exit 1; }
else
  sudo -n true 2>/dev/null || { echo "$user needs passwordless sudo (the runners use it)" >&2; exit 1; }
fi

# a panel started by hand would hold the port
pid=$(ss -ltnp 2>/dev/null | grep ':8765 ' | grep -oE 'pid=[0-9]+' | cut -d= -f2 | head -1 || true)
if [ -n "$pid" ] && ! systemctl is-active -q "$NAME"; then
  echo "stopping the panel started by hand (pid $pid); running jobs are not affected"
  $SUDO kill "$pid" || true
  for _ in 1 2 3 4 5 6 7 8 9 10; do ss -ltn | grep -q ':8765 ' || break; sleep 0.5; done
fi

# systemd opens the log itself before the service starts: the folder and file must exist, owned by the user
grp=$(id -gn "$user")
$SUDO install -d -o "$user" -g "$grp" "$home/.experiment_panel"
$SUDO touch "$home/.experiment_panel/panel.log"
$SUDO chown "$user:$grp" "$home/.experiment_panel/panel.log"

sed -e "s|@USER@|$user|g" -e "s|@HOME@|$home|g" -e "s|@REPO@|$repo|g" "$here/$NAME.service" | $SUDO tee "$UNIT" > /dev/null
$SUDO systemctl daemon-reload
$SUDO systemctl enable "$NAME" > /dev/null 2>&1
$SUDO systemctl restart "$NAME"
for _ in $(seq 1 20); do [ -s "$home/.experiment_panel/token" ] && ss -ltn | grep -q ':8765 ' && break; sleep 0.5; done
systemctl --no-pager --lines=0 status "$NAME" | head -5 || true
tok=$(cat "$home/.experiment_panel/token" 2>/dev/null || true)
ip=$(curl -s -m 3 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')
url="http://$ip:8765/?token=$tok"
echo "$url" > "$home/.experiment_panel/url" && chown "$user:$grp" "$home/.experiment_panel/url" 2>/dev/null || true
echo
echo "panel: $url   (also in $home/.experiment_panel/url)"
echo "logs:  $home/.experiment_panel/panel.log   (systemctl status $NAME)"
