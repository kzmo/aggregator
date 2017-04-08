import pika
import ConfigParser
import threading
import json
import time
import copy

class Scatterer(threading.Thread):
    def __init__(self, hostname, unitids, pollinterval, datastorage):
        super(Scatterer, self).__init__()

        print "Starting scatterer.."
        
        self.unitids = unitids
        self.pollinterval = pollinterval
        self.datastorage = datastorage

        print "Opening RabbitMQ connection, hostname: "+hostname
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
        self.channel = self.connection.channel()

        print "Creating exchange 'units'.."
        self.channel.exchange_declare(exchange='units', type='direct')

        self.running = True
        self.start()

    def run(self):
        print "Scatterer started"
        
        print "Polling for units every "+str(self.pollinterval)+" seconds"
        print "Units to be polled: "+', '.join(self.unitids)

        starttime = 0
        while self.running:
            if time.time() - starttime > self.pollinterval:
                print "Sending queries to all units.."

                for unitid in self.unitids:
                    print "Querying unit: "+unitid
                    self.datastorage.query_started(unitid)
                    self.channel.basic_publish(exchange='units', routing_key=unitid, body='status')
                    
                starttime = time.time()

        print "Scatterer killed"

    def kill(self):
        print "Killing scatterer.."
        self.running = False

class Gatherer(threading.Thread):
    def __init__(self, hostname, datastorage):
        super(Gatherer, self).__init__()

        print "Starting gatherer.."

        self.datastorage = datastorage
        print "Opening RabbitMQ connection, hostname: "+hostname
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
        self.channel = self.connection.channel()

        self.channel.queue_delete(queue='states')
        self.channel.queue_declare(queue='states', durable=False)
        self.channel.basic_consume(self.status_callback_gen(), queue='states')

        self.start()

    def status_callback_gen(self):
        gatherer = self
        def status_callback(ch, method, properties, body):
            try:
                data = json.loads(body)
                print "Got status: "+str(body)
                self.datastorage.put_data(data)
            except Exception as e:
                print "Invalid data. Skipping. Error: "+str(e)

        return status_callback

    def run(self):
        while self.channel._consumer_infos:
            self.channel.connection.process_data_events(time_limit=1) # 1 second
        print "Gatherer killed."

    def kill(self):
        print "Killing gatherer.."
        self.channel.stop_consuming()

class AnalyzerPoster(threading.Thread):
    def __init__(self, hostname, datastorage, results_interval, unit_timeout, minimumsoc, maximumsoc):
        super(AnalyzerPoster, self).__init__()
        self.datastorage = datastorage
        self.results_interval = results_interval
        self.unit_timeout = unit_timeout
        self.minimumsoc = minimumsoc
        self.maximumsoc = maximumsoc

        print "Opening RabbitMQ connection, hostname: "+hostname
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
        self.channel = self.connection.channel()

        print "Creating exchange 'result'.."
        self.channel.exchange_declare(exchange='result', type='fanout')

        self.running = True
        self.start()

    def run(self):
        start_time = time.time()
        data = {}
        old_data = {}
        while self.running:
            if time.time() - start_time > self.results_interval:
                old_data = data
                current_time = time.time()
                data = self.datastorage.get_all_data()

                # Check that we have responses to every query and mark as inactive if not.
                for unitid in data:
                    if not 'ReceivedTime' in data[unitid]:
                        if current_time - data[unitid]['QueryTime'] > self.unit_timeout:
                            data[unitid]['Active'] = False

                        # Copy from old data if data not outdated (no timeout)
                        elif unitid in old_data and 'ReceivedTime' in old_data[unitid]:
                            data[unitid] = old_data[unitid]
                        else:
                            # Just mark it as inactive is absolutely no information yet.
                            data[unitid]['Active'] = False

                print "data: "+str(data)

                active_count = 0
                total_count = 0
                remaining_capacity = 0.0
                soc_sum = 0.0
                out_of_boundaries = []

                for unitid in data:
                    unit = data[unitid]
                    total_count += 1
                    if not unit['Active']:
                        continue
                    soc = unit['SoC']

                    if soc < minimumsoc or soc > maximumsoc:
                        out_of_boundaries.append(unitid)

                    remaining_capacity += soc*unit['TotalCapacity']
                    soc_sum += soc
                    active_count += 1

                if active_count > 0:
                    average_soc = soc_sum / active_count
                else:
                    average_soc = 0

                json_data = json.dumps(
                    {'AverageSoC': average_soc,
                     'RemainingCapacity': remaining_capacity,
                     'NumberOfActiveUnits': active_count,
                     'NumberOfUnits': total_count,
                     'UnitsOutOfBoundaries': out_of_boundaries})

                print json_data
                self.channel.basic_publish(exchange='result', routing_key='', body=json_data)

                start_time = time.time()

        print "Analyzer/poster killed"
            
    def kill(self):
        print "Killing analyzer/poster"
        self.running = False

class DataStorage:
    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def put_data(self, insert):
        self.lock.acquire()
        try:
            if insert["UnitId"] in self.data:
                d = self.data[insert["UnitId"]]
                d["ReceivedTime"] = time.time()
                d["Active"] = insert["Active"]
                d["SoC"] = insert["SoC"]
                d["TotalCapacity"] = insert["TotalCapacity"]
        except Exception as e:
            print "Invalid data: "+str(e)
        self.lock.release()

    def query_started(self, unitid):
        self.lock.acquire()
        self.data[unitid] = {}
        self.data[unitid]['QueryTime'] = time.time()
        self.lock.release()

    def get_all_data(self):
        self.lock.acquire()
        return_data = copy.deepcopy(self.data)
        self.lock.release()
        return return_data

if __name__ == '__main__':
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
        pollinterval = float(config.get('Units','pollinterval'))
    except Exception as e:
        print "Could not parse poll interval: "+str(e)
        exit(1)

    try:
        resultsinterval = float(config.get('Results','resultsinterval'))
    except Exception as e:
        print "Could not parse results interval: "+str(e)
        exit(1)

    try:
        unittimeout = float(config.get('Results','unittimeout'))
    except Exception as e:
        print "Could not parse unit timeout: "+str(e)
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

    datastorage = DataStorage()

    scatterer = Scatterer(hostname, unitids, pollinterval, datastorage)
    gatherer = Gatherer(hostname, datastorage)
    analyzerposter = AnalyzerPoster(hostname, datastorage, resultsinterval, unittimeout, minimumsoc, maximumsoc)

    try:
        while True:
            pass
    except:
        pass

    scatterer.kill()
    gatherer.kill()
    analyzerposter.kill()
