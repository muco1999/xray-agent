#!/usr/bin/env bash
set -euo pipefail

REPO_URL="https://github.com/muco1999/xray-agent.git"
INSTALL_DIR="/opt/xray-agent"

need_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    echo "ERROR: run as root (or with sudo)" >&2
    exit 1
  fi
}

install_git() {
  echo "[1/6] Install git"
  apt update
  apt install -y git ca-certificates curl gnupg lsb-release
}

install_docker_official() {
  echo "[2/6] Install Docker (official repo) + compose plugin"

  # keyrings dir
  install -m 0755 -d /etc/apt/keyrings

  # add docker GPG key
  if [[ ! -f /etc/apt/keyrings/docker.gpg ]]; then
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
  fi

  # add docker repo
  UBUNTU_CODENAME="$(. /etc/os-release && echo "${UBUNTU_CODENAME}")"
  ARCH="$(dpkg --print-architecture)"

  cat > /etc/apt/sources.list.d/docker.list <<EOF
deb [arch=${ARCH} signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${UBUNTU_CODENAME} stable
EOF

  apt update
  apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

  systemctl enable --now docker

  echo "[OK] docker: $(docker --version)"
  echo "[OK] compose: $(docker compose version)"
}

clone_or_update_repo() {
  echo "[3/6] Clone or update repo: ${REPO_URL}"

  if [[ -d "${INSTALL_DIR}/.git" ]]; then
    cd "${INSTALL_DIR}"
    git remote set-url origin "${REPO_URL}" || true
    git pull --rebase
  else
    rm -rf "${INSTALL_DIR}"
    git clone "${REPO_URL}" "${INSTALL_DIR}"
    cd "${INSTALL_DIR}"
  fi
}

run_agent() {
  echo "[4/6] Prepare scripts"
  chmod +x bootstrap.sh run.sh || true

  echo "[5/6] Run agent"
  ./run.sh
}

post_info() {
  echo "[6/6] Done."
  echo "Project dir: ${INSTALL_DIR}"
  echo "Check containers: docker compose -f ${INSTALL_DIR}/docker-compose.yml ps"
  echo "Logs API: docker compose -f ${INSTALL_DIR}/docker-compose.yml logs -f --tail=200 xray-agent-api"
}

main() {
  need_root
  install_git
  install_docker_official
  clone_or_update_repo
  run_agent
  post_info
}

main "$@"
