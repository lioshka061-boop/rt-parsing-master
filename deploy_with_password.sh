#!/usr/bin/expect -f

set timeout 30
set password "7cC4BsyEV0Lky"
set server "root@46.254.107.103"
set remote_path "/home/rt-parsing"
set local_path "/Users/manda/Downloads/rt-parsing-master"

# Функція для завантаження файлу
proc upload_file {local_file remote_file} {
    global password server
    spawn scp -o StrictHostKeyChecking=no $local_file $server:$remote_file
    expect {
        "password:" {
            send "$password\r"
            exp_continue
        }
        "Permission denied" {
            puts "ERROR: Permission denied"
            exit 1
        }
        eof {
            puts "File uploaded: $local_file"
        }
    }
}

puts "Завантаження виправлених файлів на сервер..."

upload_file "$local_path/src/main.rs" "$remote_path/src/main.rs"
upload_file "$local_path/src/control/mod.rs" "$remote_path/src/control/mod.rs"
upload_file "$local_path/src/dt/parser.rs" "$remote_path/src/dt/parser.rs"
upload_file "$local_path/src/import_throttle.rs" "$remote_path/src/import_throttle.rs"
upload_file "$local_path/src/ddaudio_import.rs" "$remote_path/src/ddaudio_import.rs"
upload_file "$local_path/src/export.rs" "$remote_path/src/export.rs"
upload_file "$local_path/templates/control_panel/base.html" "$remote_path/templates/control_panel/base.html"

puts "\nФайли завантажено. Тепер виконую команди на сервері..."

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

send "docker compose -f $remote_path/compose.yml build rt-parsing\r"
expect "# "

send "docker compose -f $remote_path/compose.yml up -d --force-recreate rt-parsing\r"
expect "# "

send "exit\r"
expect eof

puts "\nДеплой завершено!"
