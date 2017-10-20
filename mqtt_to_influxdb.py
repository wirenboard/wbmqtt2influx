#!/usr/bin/python
import argparse
import math

try:
    import mosquitto
except ImportError:
    import paho.mqtt.client as mosquitto

import time
import random
import sys

from influxdb import InfluxDBClient

def write_data_item(client, device_id, control_id, value):
    value = value.replace('\n', ' ')
    if not value:
        return


    fields = {}
    try:
        value_f = float(value)
        if not math.isnan(value_f):
            fields["value_f"] = value_f
    except ValueError:
        pass
    fields["value_s"] = value
    try:
        fields["value_b"] = bool(value)
    except ValueError:
        pass




    item = {
        'measurement': 'mqtt_data',
        'tags' : {
            'client' : client,
            'did' : device_id,
            'cid'   : control_id,
            "channel" : '%s/%s' % (device_id, control_id),
        },
        "fields" : fields
    }

    req_body = [item,]
    print(req_body)
    influx_client.write_points(req_body)

def on_mqtt_message(arg0, arg1, arg2=None):
    if arg2 is None:
        msg = arg1
    else:
        msg = arg2

    if msg.retain:
        return

    parts = msg.topic.split('/')
    client = None
    if len(parts) < 4:
        return

    if (parts[1] == 'client'):
        client = parts[2]
        parts = parts[3:]
    
    if len(parts) != 4:
        return

    device_id = parts[1]
    control_id = parts[3]
    value = msg.payload.decode('utf8')

    write_data_item(client, device_id, control_id, value)

influx_client = None

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MQTT retained message deleter', add_help=False)

    parser.add_argument('-h', '--host', dest='host', type=str,
                        help='MQTT host', default='localhost')

    parser.add_argument('-u', '--username', dest='username', type=str,
                        help='MQTT username', default='')

    parser.add_argument('-P', '--password', dest='password', type=str,
                        help='MQTT password', default='')

    parser.add_argument('-p', '--port', dest='port', type=int,
                        help='MQTT port', default='1883')

    mqtt_device_id = str(time.time()) + str(random.randint(0, 100000))

    parser.add_argument('topic',  type=str,
                        help='Topic mask to unpublish retained messages from. For example: "/devices/my-device/#"')

    args = parser.parse_args()

    client = mosquitto.Mosquitto(client_id=None, clean_session=True)

    if args.username:
        client.username_pw_set(args.username, args.password)

    client.connect(args.host, args.port)
    client.on_message = on_mqtt_message

    client.subscribe(args.topic)


    influx_client = InfluxDBClient('localhost', 8086, database='mqtt_data')


    while 1:
        rc = client.loop()
        if rc != 0:
            break
