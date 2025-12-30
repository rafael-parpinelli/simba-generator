#!/bin/bash
set -e

APP_NAME="Gerador SIMBA CashWay.app"
VERSION="1.0.0"
DIST_DIR="dist"
DMG_NAME="Gerador_SIMBA_CashWay_${VERSION}.dmg"

# Remove DMG antigo se existir
rm -f "$DIST_DIR/$DMG_NAME"

create-dmg \
  --volname "Gerador SIMBA CashWay" \
  --window-pos 200 120 \
  --window-size 600 400 \
  --icon-size 100 \
  --icon "$APP_NAME" 200 190 \
  --hide-extension "$APP_NAME" \
  --app-drop-link 400 190 \
  "$DIST_DIR/$DMG_NAME" \
  "$DIST_DIR"
