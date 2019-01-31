# wbmqtt2influx

Скрипт для сохранения данных из mosquitto в InfluxDB.

Использовать только с Python 3.

Как использовать:
1. Создайте базу данных с названием `mqtt_data`.
2. `python3 mqtt_to_influxdb.py -h localhost -u USER -P xxxPASSWORDxxx '/client/test_topic/#'`
