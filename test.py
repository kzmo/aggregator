import pika
import threading
import argparse
import ConfigParser
import json
import random

class Unit(threading.Thread):
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
    
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))

        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='units', type='direct')

        self.channel.queue_declare(queue='states', durable=False)

        result = self.channel.queue_declare(exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange='units', queue=self.queue_name, routing_key=self.unitid)
        self.channel.basic_consume(self.status_callback_gen(), queue=self.queue_name, no_ack=True, 
                                   consumer_tag=self.unitid)

        self.running = True
        self.start()

    def status_callback_gen(self):
        unit = self
        def status_callback(ch, method, properties, body):
            print "Unit: "+unit.unitid+" got message: "+str(body)
            jsondata = json.dumps({'UnitId': unit.unitid,
                                   'Active': unit.active,
                                   'SoC': unit.soc,
                                   'TotalCapacity': unit.totalcapacity})
            print "Responding: "+str(jsondata)
            unit.channel.basic_publish(exchange='', routing_key='states', body=jsondata)

        return status_callback

    def run(self):
        while self.channel._consumer_infos:
            self.channel.connection.process_data_events(time_limit=1) # 1 second
        print "Test unit "+self.unitid+" killed."

    def kill(self):
        print "Killing unit: "+self.unitid
        self.channel.stop_consuming()

def result_callback(ch, method, properties, body):
    print "Got results: "+body

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test for the RabbitMQ aggregator')
    parser.add_argument('--active', help='Percentage active of units. Default 100', default="100")
    parser.add_argument('--oob', help='Percentage out of bound units from active. Default 0', default="0")
    parser.add_argument('--nonresponding', help='Percentage of non-active that are non-responding. Default 0', 
                        default="0")

    args = parser.parse_args()

    # Read the aggregator.ini file
    config = ConfigParser.ConfigParser()
    config.read('aggregator.ini')
    hostname = config.get('Connection','hostname')

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

    units = []
    for unitid in active_unit_ids:
        if unitid not in out_of_bounds:
            soc = random.uniform(minimumsoc, maximumsoc)
            units.append(Unit(unitid, hostname, True, soc, [1000,2000,3000][random.randint(0,2)]))
        else:
            soc = random.uniform(0, minimumsoc + 1-maximumsoc)
            if soc > minimumsoc:
                soc += maximumsoc-minimumsoc
            units.append(Unit(unitid, hostname, True, soc, [1000,2000,3000][random.randint(0,2)]))

    for unitid in inactive_unit_ids:
        if unitid in responding_inactive_unit_ids:
            units.append(Unit(unitid, hostname, False, 0, [1000,2000,3000][random.randint(0,2)]))

    print "Opening RabbitMQ connection, hostname: "+hostname
    connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
    channel = connection.channel()

    print "Creating exchange 'result'.."
    channel.exchange_declare(exchange='result', type='fanout')
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='result', queue=queue_name)
    channel.basic_consume(result_callback, queue=queue_name, no_ack=True)

    print "Start consuming.."
    #channel.start_consuming()
    try:
        while channel._consumer_infos:
            channel.connection.process_data_events(time_limit=1) # 1 second
    except:
        pass

    for unit in units:
        unit.kill()
