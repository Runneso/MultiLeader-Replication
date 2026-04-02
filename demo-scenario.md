# Demo scenarios

---

## Сценарий 1 — конфликт на одном ключе

```bash
# Запускаем 2 узла 
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082

# Запускаем CLI
./cli.exe 

# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
setRole A master
setRole B master
# Ставим ощутимую задержку для конфликта
setReplicationDelayMs 15000 15000
# Записываем на оба узла и видем расхождения сразу после записи
put <key> <value1> --target A
put <key> <value2> --target B
clusterDump
# Ждём задержку и видем результат value2 для обоих узлов, ведь 'A' < 'B', значит при tie-break выбирается версия для узла B
clusterDump
```

---

## Сценарий 2 — replication lag и stale read

```bash
# Запускаем 4 узла
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082
./node.exe -id=C -host="127.0.0.1" -port=8083
./node.exe -id=D -host="127.0.0.1" -port=8084

# Запускаем CLI
./cli.exe

# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
setRole A master
setRole B master
addNode C 127.0.0.1 8083
addNode D 127.0.0.1 8084
setRole C follower
setRole D follower
# Ставим ощутимую задержку для stale read
setReplicationDelayMs 5000 5000
# Записываем на какого-то мастера и видим stale read
put <key> <value> --target A|B
clusterDump
# Ждём задержку и видим корректное схождение
clusterDump
```

---

## Сценарий 3 — mesh vs ring

```bash
# Запускаем 3 узла
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082
./node.exe -id=C -host="127.0.0.1" -port=8083

# Запускаем CLI
./cli.exe

# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
setRole A master
setRole B master
setRole C master
setTopology mesh
# Ставим ощутимую задержку для наглядности сравнения
setReplicationDelayMs <time> <time>
# Делаем запись на любого мастера
put <key> <value> --target A|B|C
# Ждём примерно time (в общем случае O(1)) времени и видим корректное схождение
clusterDump
```


```bash
# Запускаем 3 узла
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082
./node.exe -id=C -host="127.0.0.1" -port=8083

# Запускаем CLI
./cli.exe

# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
setRole A master
setRole B master
setRole C master
setTopology ring
# Ставим ощутимую задержку для наглядности сравнения
setReplicationDelayMs <time> <time>
# Делаем запись на любого мастера
put <key> <value> --target A|B|C
# Ждём примерно 2*time (в общем случае O(n)) времени и видим корректное схождение
clusterDump
```

--- 

## Сценарий 4 — star как bottleneck / single point of failure
```bash
# Запускаем 4 узла
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082
./node.exe -id=C -host="127.0.0.1" -port=8083
./node.exe -id=D -host="127.0.0.1" -port=8084

# Запускаем CLI
./cli.exe

# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
addNode D 127.0.0.1 8084
setRole A master
setRole B master
setRole C master
setRole D master
setStarCenter A
setTopology star
# Ставим ощутимую задержку для наглядности сравнения
setReplicationDelayMs 5000 5000
# Делаем запись на любой узел, кроме центра
put <key> <value> --target B|C|D
# Убиваем центральный узел A
# Видим, что данные не дошли остальных двух узлов, ведь центр "умер"
clusterDump
```

## Сценарий 5 — ring ломается при падении узла без removeNode
```bash
# Запускаем 3 узла
./node.exe -id=A -host="127.0.0.1" -port=8081
./node.exe -id=B -host="127.0.0.1" -port=8082
./node.exe -id=C -host="127.0.0.1" -port=8083

# Запускаем CLI
./cli.exe

# Настраиваем кластер
addNode A 127.0.0.1 8081
addNode B 127.0.0.1 8082
addNode C 127.0.0.1 8083
setRole A master
setRole B master
setRole C master
setTopology ring # A -> B -> C -> A
# Ставим ощутимую задержку для наглядности сравнения
setReplicationDelayMs 5000 5000
# Делаем запись на самый первый узел
put <key> <name> --target A
# Убиваем следующий узел в цепочки B
# Видим, что данные не пошли дальше по кольцу
clusterDump
```