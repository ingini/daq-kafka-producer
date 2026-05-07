#!/bin/bash
# ============================================================
#  install.sh
#  release 디렉토리를 ~/DAQ/daq-system 에 설치
#  사용법: ./install.sh
# ============================================================
set -e

DAQ_DIR="$HOME/DAQ"
INSTALL_DIR="$DAQ_DIR/daq-system"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "[install] Source : $SCRIPT_DIR"
echo "[install] Target : $INSTALL_DIR"

# 기존 설치 백업
if [ -d "$INSTALL_DIR" ]; then
    BACKUP="$INSTALL_DIR.bak.$(date +%Y%m%d_%H%M%S)"
    echo "[install] Backing up existing install → $BACKUP"
    mv "$INSTALL_DIR" "$BACKUP"
fi

mkdir -p "$DAQ_DIR"
cp -r "$SCRIPT_DIR" "$INSTALL_DIR"

echo "[install] Done. Run: cd $INSTALL_DIR && ./run.sh"
