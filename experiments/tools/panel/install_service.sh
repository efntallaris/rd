#!/usr/bin/env bash
# Installs the experiment panel as a systemd service that starts at boot (and restarts if it dies).
#
#   experiments/tools/panel/install_service.sh            # install + start, as the calling user
#   experiments/tools/panel/install_service.sh --remove   # stop + uninstall
#
# Run it as the user that runs the experiments (not as root): the panel calls sudo itself.
set -euo pipefail
NAME=experiment-panel
UNIT=/etc/systemd/system/$NAME.service
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)
user=${SUDO_USER:-$(id -un)}
home=$(getent passwd "$user" | cut -d: -f6)

if [ "${1:-}" = "--remove" ]; then
  sudo systemctl disable --now "$NAME" 2>/dev/null || true
  sudo rm -f "$UNIT"
  sudo systemctl daemon-reload
  echo "removed $NAME"
  exit 0
fi
[ "$user" = root ] && { echo "run this as the experiment user, not root" >&2; exit 1; }
sudo -n true 2>/dev/null || { echo "$user needs passwordless sudo (the runners use it)" >&2; exit 1; }

# a panel started by hand would hold the port
pid=$(ss -ltnp 2>/dev/null | grep ':8765 ' | grep -oE 'pid=[0-9]+' | cut -d= -f2 | head -1 || true)
if [ -n "$pid" ] && ! systemctl is-active -q "$NAME"; then
  echo "stopping the panel started by hand (pid $pid); running jobs are not affected"
  kill "$pid"; for _ in 1 2 3 4 5 6 7 8 9 10; do ss -ltn | grep -q ':8765 ' || break; sleep 0.5; done
fi

sed -e "s|@USER@|$user|g" -e "s|@HOME@|$home|g" -e "s|@REPO@|$repo|g" "$here/$NAME.service" | sudo tee "$UNIT" > /dev/null
sudo systemctl daemon-reload
sudo systemctl enable "$NAME" > /dev/null 2>&1
sudo systemctl restart "$NAME"
for _ in $(seq 1 20); do ss -ltn | grep -q ':8765 ' && break; sleep 0.5; done
systemctl --no-pager --lines=0 status "$NAME" | head -5
tok=$(cat "$home/.experiment_panel/token" 2>/dev/null || true)
ip=$(curl -s -m 3 ifconfig.me 2>/dev/null || hostname -I | awk '{print $1}')
echo
echo "panel: http://$ip:8765/?token=$tok"
echo "logs:  $home/.experiment_panel/panel.log   (systemctl status $NAME)"
