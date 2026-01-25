#!/bin/bash
# Скрипт для завантаження виправлених файлів на сервер

SERVER="root@46.254.107.103"
REMOTE_PATH="/home/rt-parsing"
LOCAL_PATH="/Users/manda/Downloads/rt-parsing-master"

echo "Завантаження виправлених файлів на сервер..."

# Основні виправлені файли
scp "$LOCAL_PATH/src/main.rs" "$SERVER:$REMOTE_PATH/src/main.rs"
scp "$LOCAL_PATH/src/control/mod.rs" "$SERVER:$REMOTE_PATH/src/control/mod.rs"
scp "$LOCAL_PATH/src/dt/parser.rs" "$SERVER:$REMOTE_PATH/src/dt/parser.rs"
scp "$LOCAL_PATH/src/import_throttle.rs" "$SERVER:$REMOTE_PATH/src/import_throttle.rs"
scp "$LOCAL_PATH/src/ddaudio_import.rs" "$SERVER:$REMOTE_PATH/src/ddaudio_import.rs"
scp "$LOCAL_PATH/src/export.rs" "$SERVER:$REMOTE_PATH/src/export.rs"
scp "$LOCAL_PATH/templates/control_panel/base.html" "$SERVER:$REMOTE_PATH/templates/control_panel/base.html"

echo "Файли завантажено. Тепер виконайте на сервері:"
echo ""
echo "ssh $SERVER"
echo "cd $REMOTE_PATH"
echo "docker compose -f $REMOTE_PATH/compose.yml build rt-parsing"
echo "docker compose -f $REMOTE_PATH/compose.yml up -d --force-recreate rt-parsing"
