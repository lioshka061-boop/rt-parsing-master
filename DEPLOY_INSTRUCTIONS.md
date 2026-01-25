# Інструкції для завантаження виправлень на сервер

## Файли, які були виправлені:

1. `src/main.rs` - виправлено обробку помилок, прибрано логування секретного ключа, оптимізовано підключення до БД
2. `src/control/mod.rs` - виправлено зайву змінну `mut`
3. `src/dt/parser.rs` - замінено `panic!` на правильну обробку помилок
4. `src/import_throttle.rs` - покращена обробка помилок семафора
5. `src/ddaudio_import.rs` - виправлено unreachable statement
6. `src/export.rs` - виправлено зайві дужки
7. `templates/control_panel/base.html` - виправлено опечатку "settins" → "settings"

## Команди для завантаження:

```bash
cd /Users/manda/Downloads/rt-parsing-master

# Завантаження виправлених файлів
scp src/main.rs root@46.254.107.103:/home/rt-parsing/src/main.rs
scp src/control/mod.rs root@46.254.107.103:/home/rt-parsing/src/control/mod.rs
scp src/dt/parser.rs root@46.254.107.103:/home/rt-parsing/src/dt/parser.rs
scp src/import_throttle.rs root@46.254.107.103:/home/rt-parsing/src/import_throttle.rs
scp src/ddaudio_import.rs root@46.254.107.103:/home/rt-parsing/src/ddaudio_import.rs
scp src/export.rs root@46.254.107.103:/home/rt-parsing/src/export.rs
scp templates/control_panel/base.html root@46.254.107.103:/home/rt-parsing/templates/control_panel/base.html

# Підключення до сервера та перебудова
ssh root@46.254.107.103
cd /home/rt-parsing
docker compose -f /home/rt-parsing/compose.yml build rt-parsing
docker compose -f /home/rt-parsing/compose.yml up -d --force-recreate rt-parsing
```

## Альтернативний спосіб (якщо є доступ через git):

Якщо на сервері є git репозиторій, можна виконати:

```bash
ssh root@46.254.107.103
cd /home/rt-parsing
git pull  # або git fetch + git merge
docker compose -f /home/rt-parsing/compose.yml build rt-parsing
docker compose -f /home/rt-parsing/compose.yml up -d --force-recreate rt-parsing
```

## Перевірка після деплою:

Після перезапуску контейнера перевірте логи:

```bash
ssh root@46.254.107.103
docker compose -f /home/rt-parsing/compose.yml logs rt-parsing --tail=50
```

Переконайтеся, що:
- Сервер запускається без помилок
- Немає повідомлень про panic
- Підключення до БД успішні
- Адмін панель працює коректно (перевірте `/control_panel/settings` - опечатка має бути виправлена)
