#!/usr/bin/env bash
set -Eeuo pipefail
umask 027

# ============================================================
# Installazione di gs-agent come servizio systemd (Rocky / RHEL / Debian / Ubuntu).
#
#   sudo bash install.sh --hub https://<hub> --token gsat_...
#
# Da lanciare dalla cartella del pacchetto gs-agent (con package.json e dist/ gia' compilati,
# oppure con i sorgenti: lo script esegue npm ci + build). Richiede Node.js 22 o superiore.
# Senza --token installa solo il servizio; l'attivazione si fa dopo con:
#   sudo -u gs-agent node /opt/gs-agent/dist/cli.js enroll --hub ... --token ... --data-dir /var/lib/gs-agent
# Rilanciabile: aggiorna il codice e riavvia il servizio, l'attivazione resta.
# ============================================================

APP_DIR="/opt/gs-agent"
DATA_DIR="/var/lib/gs-agent"
SERVICE_USER="gs-agent"
UNIT="/etc/systemd/system/gs-agent.service"

HUB=""
TOKEN=""

log()  { printf '\n\033[1;32m==> %s\033[0m\n' "$*"; }
warn() { printf '\n\033[1;33mATTENZIONE: %s\033[0m\n' "$*" >&2; }
die()  { printf '\n\033[1;31mERRORE: %s\033[0m\n' "$*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --hub) HUB="${2:-}"; shift 2 ;;
    --token) TOKEN="${2:-}"; shift 2 ;;
    *) die "opzione sconosciuta: $1" ;;
  esac
done

[[ "${EUID}" -eq 0 ]] || die "Esegui lo script come root: sudo bash $0"
[[ -f package.json ]] || die "Lancia lo script dalla cartella del pacchetto gs-agent"
[[ -n "${TOKEN}" && -z "${HUB}" ]] && die "--token richiede anche --hub"

command -v node >/dev/null 2>&1 || die "Node.js non trovato (serve la versione 22 o superiore)"
NODE_MAJOR="$(node -p 'process.versions.node.split(".")[0]')"
[[ "${NODE_MAJOR}" -ge 22 ]] || die "Node.js ${NODE_MAJOR} troppo vecchio: serve la 22 o superiore"
NODE_BIN="$(command -v node)"

log "Utente di servizio ${SERVICE_USER}"
if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --home-dir "${DATA_DIR}" --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

log "Codice in ${APP_DIR}"
install -d -m 0755 "${APP_DIR}"
cp -r package.json package-lock.json "${APP_DIR}/"
[[ -d dist ]] && cp -r dist "${APP_DIR}/"
[[ -d src ]] && cp -r src tsconfig.json "${APP_DIR}/"
(
  cd "${APP_DIR}"
  if [[ -d src ]]; then
    npm ci --no-audit --no-fund
    npm run build
    npm prune --omit=dev --no-audit --no-fund
  else
    npm ci --omit=dev --no-audit --no-fund
  fi
)
chown -R root:root "${APP_DIR}"

log "Dati in ${DATA_DIR} (solo ${SERVICE_USER})"
install -d -m 0700 -o "${SERVICE_USER}" -g "${SERVICE_USER}" "${DATA_DIR}"

if [[ -n "${TOKEN}" ]]; then
  log "Attivazione dell'agente"
  sudo -u "${SERVICE_USER}" "${NODE_BIN}" "${APP_DIR}/dist/cli.js" enroll --hub "${HUB}" --token "${TOKEN}" --data-dir "${DATA_DIR}"
fi

log "Unita' systemd"
cat > "${UNIT}" <<EOF
[Unit]
Description=GlueSuite agent
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=${SERVICE_USER}
Group=${SERVICE_USER}
Environment=GS_AGENT_DATA_DIR=${DATA_DIR}
Environment=NODE_ENV=production
ExecStart=${NODE_BIN} ${APP_DIR}/dist/cli.js run
Restart=always
RestartSec=5

# protezioni: l'agente scrive solo nella sua cartella dati
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true
ReadWritePaths=${DATA_DIR}
CapabilityBoundingSet=
AmbientCapabilities=

[Install]
WantedBy=multi-user.target
EOF
chmod 0644 "${UNIT}"

systemctl daemon-reload
systemctl enable gs-agent >/dev/null

if [[ -f "${DATA_DIR}/identity.json" ]]; then
  systemctl restart gs-agent
  log "Servizio avviato"
  sleep 2
  systemctl --no-pager --lines=5 status gs-agent || true
else
  warn "Agente non ancora attivato: il servizio partira' dopo l'attivazione."
  cat <<EOF
  sudo -u ${SERVICE_USER} ${NODE_BIN} ${APP_DIR}/dist/cli.js enroll --hub https://<hub> --token gsat_... --data-dir ${DATA_DIR}
  sudo systemctl start gs-agent
EOF
fi

cat <<EOF

============================================================
gs-agent installato
  Codice:  ${APP_DIR}
  Dati:    ${DATA_DIR}   (chiave privata e segreti: non copiarli altrove)
  Log:     journalctl -u gs-agent -f
  Stato:   sudo -u ${SERVICE_USER} ${NODE_BIN} ${APP_DIR}/dist/cli.js status --data-dir ${DATA_DIR}
============================================================
EOF
