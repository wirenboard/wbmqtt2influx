#!/usr/bin/env python3
import argparse
import math

import paho.mqtt.client

import time
import random
import sys

from influxdb import InfluxDBClient
from threading import Thread
from collections import deque

import logging
logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s', level=logging.INFO)

class DBWriterThread(Thread):
    def __init__(self, influx_client, *args, **kwargs):
        self.influx_client = influx_client
        self.data_queue = deque()

        super(DBWriterThread, self).__init__(*args, **kwargs)

    def schedule_item(self, client, device_id, control_id, value):
        item = (client, device_id, control_id, value)
        self.data_queue.append(item)

    def get_items(self, mininterval, maxitems):
        """ This will collect items from queue until either 'mininterval' 
        is over or 'maxitems' items are collected """
        started = time.time()
        items = []

        while (time.time() - started < mininterval) and (len(items) < maxitems):
            try:
                item = self.data_queue.popleft()
            except IndexError:
                time.sleep(mininterval * 0.1)
            else:
                items.append(item)

        return items

    def run(self):
        while True:
            items = self.get_items(mininterval=0.05, maxitems=50)
            db_req_body = []
            stat_clients = set()
            for client, device_id, control_id, value in items:
                ser_item = self.serialize_data_item(client, device_id, control_id, value)
                if ser_item:
                    db_req_body.append(ser_item)
                    stat_clients.add(client)

            if db_req_body:
                logging.info("Write %d items for %d clients" % (len(items), len(stat_clients)))
                self.influx_client.write_points(db_req_body)

        time.sleep(0.01)



    def serialize_data_item(self, client, device_id, control_id, value):
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
        if "value_f" not in fields:
            fields["value_s"] = value

        item = {
            'measurement': 'mqtt_data',
            'tags' : {
                'client' : client,
                "channel" : '%s/%s' % (device_id, control_id),
            },
            "fields" : fields
        }

        return item

db_writer = None

def on_mqtt_message(arg0, arg1, arg2=None):
    if arg2 is None:
        msg = arg1
    else:
        msg = arg2

    if msg.retain:
        return

    parts = msg.topic.split('/')[1:]
    client = None
    if len(parts) < 4:
        return

    if (parts[0] == 'client'):
        client = parts[1]
        parts = parts[2:]

    if len(parts) != 4:
        return

    device_id = parts[1]
    control_id = parts[3]
    value = msg.payload.decode('utf8')


    db_writer.schedule_item(client, device_id, control_id, value)


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

    client = paho.mqtt.client.Client(client_id=None, clean_session=True, protocol=paho.mqtt.client.MQTTv31)

    if args.username:
        client.username_pw_set(args.username, args.password)

    client.connect(args.host, args.port)
    client.on_message = on_mqtt_message

    client.subscribe(args.topic)


    influx_client = InfluxDBClient('localhost', 8086, database='mqtt_data')
    db_writer =  DBWriterThread(influx_client, daemon=True)
    db_writer.start()


    while 1:
        rc = client.loop()
        if rc != 0:
            break
