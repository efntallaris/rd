#!/usr/bin/env bash
# CloudLab startup hook: sets up the experiment panel (web dashboard on :8765) on the controller.
#
# Called, as root, at the end of scripts_copy/install_preqs.sh, which the CloudLab profile runs
# on every node at every boot. Does nothing on the other nodes. Safe to run again.
#
#   PANEL_CONTROLLER  node that runs the panel (default redis0: it runs the playbooks)
#   PANEL_USER        user that runs it (default: the owner of the /users/<user>/rd checkout)
set -uo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)
controller=${PANEL_CONTROLLER:-redis0}
if [ "$(hostname -s)" != "$controller" ]; then
  echo "experiment panel: $(hostname -s) is not the controller ($controller), skipped"
  exit 0
fi
user=${PANEL_USER:-$(basename "$(dirname "$repo")")}   # /users/<user>/rd
echo "experiment panel: setting up on $(hostname -s) for $user ($repo)"
SUDO=sudo; [ "$(id -u)" = 0 ] && SUDO=""
python3 -c 'import yaml' 2> /dev/null || $SUDO apt-get install -y python3-yaml   # topology from the inventories
"$here/install_service.sh" --user "$user"
