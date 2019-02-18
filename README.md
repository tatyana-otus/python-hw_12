# MemcLoad v2
Задание: нужно переписать Python версию memc_load.py на Go. Программа по‐прежнему парсит и заливает в
мемкеш поминутную выгрузку логов трекера установленных приложений. Ключом является тип и идентификатор
устройства через двоеточие, значением являет protobuf сообщение (https://github.com/golang/protobuf).

## Testing
```
go test -v ./...
```

## Usage
```
go build -o memcload main.go
./memcload --pattern=data/*.tsv.gz
```

