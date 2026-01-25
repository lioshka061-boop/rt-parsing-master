#!/usr/bin/expect -f

set timeout 300
set password "7cC4BsyEV0Lky"
set server "root@46.254.107.103"
set remote_path "/home/rt-parsing"

puts "Підключення до сервера та виконання команд перебудови..."

spawn ssh -o StrictHostKeyChecking=no $server
expect {
    "password:" {
        send "$password\r"
    }
    "Permission denied" {
        puts "ERROR: Permission denied"
        exit 1
    }
}

expect "# "
send "cd $remote_path\r"
expect "# "

puts "Виконую перебудову контейнера..."
send "docker compose -f $remote_path/compose.yml build rt-parsing\r"
expect {
    "# " {
        puts "Перебудова завершена"
    }
    timeout {
        puts "Попередження: перебудова займає багато часу..."
        expect "# "
    }
}

puts "Перезапускаю контейнер..."
send "docker compose -f $remote_path/compose.yml up -d --force-recreate rt-parsing\r"
expect "# "

puts "Перевіряю статус контейнера..."
send "docker compose -f $remote_path/compose.yml ps rt-parsing\r"
expect "# "

puts "Переглядаю останні логи..."
send "docker compose -f $remote_path/compose.yml logs rt-parsing --tail=20\r"
expect "# "

send "exit\r"
expect eof

puts "\n✅ Деплой завершено успішно!"
