'''
test.py

A simple test system to test that the aggregator.py is working.
Creates 'Unit' thread for each simulated unit.

The unit information comes from aggregator.ini.

Commandline parameters:
  -h, --help            show this help message and exit
  --active ACTIVE       Percentage active of units. Default 100
  --oob OOB             Percentage out of bound units from active. Default 0
  --nonresponding NONRESPONDING
                        Percentage of non-active that are non-responding.
                        Default 0

Copyright 2017 Janne Valtanen
'''

import pika
import threading
import argparse
import ConfigParser
import json
import random

class Unit(threading.Thread):
    '''
    Unit thread class that represents a single unit.

    Parameters:
    - unitid: Unit ID in a string
    - hostname: The hostname for the MQTT server
    - active: Boolean telling if this unit is active or not.
    - soc: The SoC value.
    - totalcapacity: Total capacity in kWh.
    '''

    def __init__(self, unitid, hostname, active, soc, totalcapacity):
        super(Unit, self).__init__()
        self.unitid = unitid
        self.active = active
        self.soc = soc
        self.totalcapacity = totalcapacity
        print "-----------------------------"
        print "Starting test unit: "+self.unitid
        print "Active: "+str(self.active)
        print "SoC: "+str(self.soc)
        print "Total capacity (kWh): "+str(self.totalcapacity)
        print "Opening RabbitMQ connection, hostname: "+hostname
    
        # Setup the MQTT connection and declare 'units' and 'states'
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange='units', type='direct')
        self.channel.queue_declare(queue='states', durable=False)

        result = self.channel.queue_declare(exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange='units', queue=self.queue_name, routing_key=self.unitid)
        self.channel.basic_consume(self.status_callback_gen(), queue=self.queue_name, no_ack=True, 
                                   consumer_tag=self.unitid)

        # Start the thread.
        self.running = True
        self.start()

    def status_callback_gen(self):
        '''
        A callback generator for status query messages for the 'units' exchange.

        Returns: The 'units' exchange callback.
        '''

        # Store self to be used in the callback
        unit = self
        def status_callback(ch, method, properties, body):
            '''
            The actual callback that will send the results from the unit back to 
            aggregator.
            '''

            # Store to JSON and publish.
            jsondata = json.dumps({'UnitId': unit.unitid,
                                   'Active': unit.active,
                                   'SoC': unit.soc,
                                   'TotalCapacity': unit.totalcapacity})
            unit.channel.basic_publish(exchange='', routing_key='states', body=jsondata)

        return status_callback

    def run(self):
        '''
        The run method of the thread. Just consume all messages until killed.
        '''
        while self.channel._consumer_infos:
            self.channel.connection.process_data_events(time_limit=1) # 1 second
        print "Test unit "+self.unitid+" killed."

    def kill(self):
        '''
        A method to kill the thread.
        '''
        print "Killing unit: "+self.unitid
        self.channel.stop_consuming()

def result_callback(ch, method, properties, body):
    # Results callback that will just display the results on console.
    print "Got results from aggerator: "+body

if __name__ == '__main__':
    # Set up the arguments.
    parser = argparse.ArgumentParser(description='Test for the RabbitMQ aggregator')
    parser.add_argument('--active', help='Percentage active of units. Default 100', default="100")
    parser.add_argument('--oob', help='Percentage out of bound units from active. Default 0', default="0")
    parser.add_argument('--nonresponding', help='Percentage of non-active that are non-responding. Default 0', 
                        default="0")

    args = parser.parse_args()

    # Read the node information from aggregator.ini file.
    config = ConfigParser.ConfigParser()
    config.read('aggregator.ini')
    hostname = config.get('Connection','hostname')

    # Check that we can read everything.
    try:
        unitids = json.loads(config.get('Units','unitids'))
    except Exception as e:
        print "Could not parse unit ID list: "+str(e)
        exit(1)

    try:
        minimumsoc = float(config.get('Units', 'minimumsoc'))
    except Exception as e:
        print "Could not parse minimum SoC: "+str(e)
        exit(1)

    try:
        maximumsoc = float(config.get('Units', 'maximumsoc'))
    except Exception as e:
        print "Could not parse maximum SoC: "+str(e)
        exit(1)

        
    # Select the units based on fail rates from command line.
    active_percentage = float(args.active)
    active_unit_ids = unitids[0: int(round(len(unitids)*active_percentage/100.0))]

    oob_percentage = float(args.oob)
    out_of_bounds = active_unit_ids[0: int(round(len(active_unit_ids)*oob_percentage/100.0))]

    inactive_unit_ids = filter(lambda x: x not in active_unit_ids, unitids)
    responding_inactive_percentage = 100.0-float(args.nonresponding)
    responding_inactive_unit_ids = \
        inactive_unit_ids[0: int(round(len(inactive_unit_ids)*responding_inactive_percentage/100.0))]    

    print 'Active units: '+', '.join(active_unit_ids)
    print 'Out of bound units: '+', '.join(out_of_bounds)
    print 'Inactive units: '+', '.join(inactive_unit_ids)
    print 'Responding inactive units: '+'. '.join(responding_inactive_unit_ids)

    # Set up the test units.
    units = []
    # Active units.
    for unitid in active_unit_ids:
        # Inside bounds.
        if unitid not in out_of_bounds:
            soc = random.uniform(minimumsoc, maximumsoc)
            units.append(Unit(unitid, hostname, True, soc, [1000,2000,3000][random.randint(0,2)]))
        # Out of bounds.
        else:
            soc = random.uniform(0, minimumsoc + 1-maximumsoc)
            if soc > minimumsoc:
                soc += maximumsoc-minimumsoc
            units.append(Unit(unitid, hostname, True, soc, [1000,2000,3000][random.randint(0,2)]))

    # Inactive units.
    for unitid in inactive_unit_ids:
        # If the unit is inactive but responsive create the instance.
        if unitid in responding_inactive_unit_ids:
            units.append(Unit(unitid, hostname, False, 0, [1000,2000,3000][random.randint(0,2)]))

    # Set up the MQTT connection for the results gathering.
    print "Opening RabbitMQ connection, hostname: "+hostname
    connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
    channel = connection.channel()

    channel.exchange_declare(exchange='result', type='fanout')
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='result', queue=queue_name)
    channel.basic_consume(result_callback, queue=queue_name, no_ack=True)

    # Run until killed.
    try:
        # This is easier to kill than the start_consume.
        while channel._consumer_infos:
            channel.connection.process_data_events(time_limit=1)
    except:
        pass

    # If killed then kill all the units also.
    for unit in units:
        unit.kill()
