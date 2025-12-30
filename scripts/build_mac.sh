#!/bin/bash
set -e

APP_NAME="Gerador SIMBA CashWay"
ICON="assets/simba_icon.icns"
VENV="venv311"

source $VENV/bin/activate

rm -rf build dist
rm -rf ~/Library/Application\ Support/pyinstaller

pyinstaller app.py \
  --onefile \
  --windowed \
  --noconfirm \
  --clean \
  --name "$APP_NAME" \
  --icon "$ICON" \
  --collect-all PyQt5 \
  --exclude-module PyQt5.QtWebEngine \
  --exclude-module PyQt5.QtWebEngineCore \
  --exclude-module PyQt5.Qt3DCore \
  --exclude-module PyQt5.Qt3DRender \
  --exclude-module PyQt5.Qt3DInput \
  --exclude-module PyQt5.Qt3DLogic \
  --exclude-module PyQt5.Qt3DExtras
