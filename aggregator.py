'''
aggregator.py

An aggregator for a scatter-gather pattern collecting state of charge information from units.

The configuration comes from the 'aggregator.ini'.

Copyright 2017 Janne Valtanen
'''

import pika
import ConfigParser
import threading
import json
import time
import copy

class Scatterer(threading.Thread):
    '''
    The Scatterer class that implements the scatter part of the scatter-gather pattern.
    Runs as a separate thread.

    Parameters:
    - hostname: The hostname of the MQTT server
    - unitids: A list of strings containing the unit IDs to be polled
    - pollinterval: The polling interval in seconds.
    - datastorage: DataStorage instance where the data will be stored.
    '''

    def __init__(self, hostname, unitids, pollinterval, datastorage):
        super(Scatterer, self).__init__()

        print "Starting scatterer.."
        
        self.unitids = unitids
        self.pollinterval = pollinterval
        self.datastorage = datastorage

        # Set up the MQTT connection and 'units' exchange
        print "Opening RabbitMQ connection, hostname: "+hostname
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='units', type='direct')

        # Start thread
        self.running = True
        self.start()

    def run(self):
        '''
        The 'run' part of the thread that sends out the state queries on a set interval.
        '''

        print "Scatterer started"
        print "Polling for units every "+str(self.pollinterval)+" seconds"
        print "Units to be polled: "+', '.join(self.unitids)

        # Poll according to the interval setting.
        starttime = 0
        while self.running:
            if time.time() - starttime > self.pollinterval:
                for unitid in self.unitids:
                    # Mark in the data storage where and when the query was sent.
                    self.datastorage.query_started(unitid)
                    # Send the query.
                    self.channel.basic_publish(exchange='units', routing_key=unitid, body='status')
                starttime = time.time()

        # If killed just exit with message.
        print "Scatterer killed"

    def kill(self):
        '''
        A method to kill the thread.
        '''

        print "Killing scatterer.."
        self.running = False

class Gatherer(threading.Thread):
    '''
    The Gatherer class that implements the gather part of the scatter-gather pattern.
    Note: This class does not yet analyze the results. AnalyzerPoster is for that purpose. 
    Runs as a separate thread. 

    Parameters:
    - hostname: The hostname of the MQTT server
    - datastorage: DataStorage instance where the data will be stored.
    '''

    def __init__(self, hostname, datastorage):
        super(Gatherer, self).__init__()

        print "Starting gatherer.."

        # Open the MQTT connection and setup queue 'states' for responses
        self.datastorage = datastorage
        print "Opening RabbitMQ connection, hostname: "+hostname
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
        self.channel = self.connection.channel()

        self.channel.queue_delete(queue='states')
        self.channel.queue_declare(queue='states', durable=False)
        self.channel.basic_consume(self.status_callback_gen(), queue='states')

        self.start()

    def status_callback_gen(self):
        '''
        A callback generator for status messages for the 'states' queue.

        Returns: The 'states' queue callback.
        '''
        
        # Store self to be used in the status callback
        gatherer = self

        def status_callback(ch, method, properties, body):
            '''
            This is the actual 'states' queue callback.
            A standard callback.
            '''

            # Try to unpack the JSON data and put into the datastorage instance.
            try:
                data = json.loads(body)
                self.datastorage.put_data(data)
            except Exception as e:
                print "Invalid data. Skipping. Error: "+str(e)

        return status_callback

    def run(self):
        '''
        The run method of the thread. Just consume all messages until killed.
        '''
        
        # A small hack to make this easier to kill.
        while self.channel._consumer_infos:
            self.channel.connection.process_data_events(time_limit=1)
        print "Gatherer killed."

    def kill(self):
        '''
        A method to kill the thread.
        '''

        print "Killing gatherer.."
        self.channel.stop_consuming()

class AnalyzerPoster(threading.Thread):
    '''
    The AnalyzerPoster class. This is the thread that analyzes the results data and posts it to MQTT.
    
    Parameters:
    - hostname: The hostname of the MQTT server
    - datastorage: DataStorage instance where the data is stored.
    - results_interval: How often the results are posted. In seconds.
    - unit_timeout: Timeout value when units are considered inactive. In seconds.
    - minimumsoc: Minimum SoC. Values below this are out of bounds.
    - maximumsoc: Maximum SoC. Valus above this are out of bounds.
    '''
    def __init__(self, hostname, datastorage, results_interval, unit_timeout, minimumsoc, maximumsoc):
        super(AnalyzerPoster, self).__init__()
        self.datastorage = datastorage
        self.results_interval = results_interval
        self.unit_timeout = unit_timeout
        self.minimumsoc = minimumsoc
        self.maximumsoc = maximumsoc

        # Set up the MQTT and declare the 'result' exchange.
        print "Opening RabbitMQ connection, hostname: "+hostname
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(hostname))
        self.channel = self.connection.channel()

        self.channel.exchange_declare(exchange='result', type='fanout')

        # Start the thread.
        self.running = True
        self.start()

    def run(self):
        '''
        The run method of the thread. Periodically check the results and post to MQTT.
        '''

        start_time = time.time()
        data = {}
        old_data = {}
        while self.running:
            # If interval has passed then start analyzing the results.
            if time.time() - start_time > self.results_interval:
                # Store the previous data in case all results from latest query have not 
                # been received yet.
                old_data = data
                current_time = time.time()
                # Get data from storage.
                data = self.datastorage.get_all_data()

                # Check that we have responses to every query and mark as inactive if not.
                for unitid in data:
                    if not 'ReceivedTime' in data[unitid]:
                        if current_time - data[unitid]['QueryTime'] > self.unit_timeout:
                            data[unitid]['Active'] = False

                        # Copy from old data if data not outdated (no timeout).
                        elif unitid in old_data and 'ReceivedTime' in old_data[unitid]:
                            data[unitid] = old_data[unitid]
                        else:
                            # Just mark it as inactive is absolutely no information yet.
                            data[unitid]['Active'] = False

                # Analyze the values in the data set.
                active_count = 0
                total_count = 0
                remaining_capacity = 0.0
                soc_sum = 0.0
                out_of_boundaries = []

                for unitid in data:
                    unit = data[unitid]
                    # All known units count in the total count.
                    total_count += 1
                    # If unit is not active then just skip it.
                    if not unit['Active']:
                        continue
                    
                    # Get the SoC value.
                    soc = unit['SoC']

                    # Check the SoC against boundaries.
                    if soc < minimumsoc or soc > maximumsoc:
                        out_of_boundaries.append(unitid)

                    # Add to remaining capacity, SoC sum and active unit count.
                    remaining_capacity += soc*unit['TotalCapacity']
                    soc_sum += soc
                    active_count += 1

                # Calculate average SoC. If possible.
                if active_count > 0:
                    average_soc = soc_sum / active_count
                else:
                    average_soc = 0

                # Pack results to JSON and post to MQTT.
                json_data = json.dumps(
                    {'AverageSoC': average_soc,
                     'RemainingCapacity': remaining_capacity,
                     'NumberOfActiveUnits': active_count,
                     'NumberOfUnits': total_count,
                     'UnitsOutOfBoundaries': out_of_boundaries})

                self.channel.basic_publish(exchange='result', routing_key='', body=json_data)

                start_time = time.time()

        print "Analyzer/poster killed"
            
    def kill(self):
        '''
        A method to kill the thread.
        '''
        print "Killing analyzer/poster"
        self.running = False

class DataStorage:
    '''
    A DataStorage class that implements a data storage with a lock.
    Also takes care of the start times of query messages.
    Can be used from multiple threads simultaneously.
    '''

    def __init__(self):
        # Initialize the data and the lock.
        self.data = {}
        self.lock = threading.Lock()

    def put_data(self, insert):
        '''
        Put data to the storage. 
        Note: The unit ID needs to be already in the storage. Put there 
              by the query_started method.

        Parameters:
        - insert: The data in the dictionary to be inserted.
        '''

        self.lock.acquire()
        try:
            # If UnitId is found then put the data in the storage.
            if insert["UnitId"] in self.data:
                d = self.data[insert["UnitId"]]
                # Mark also the received timestamp.
                d["ReceivedTime"] = time.time()
                d["Active"] = insert["Active"]
                d["SoC"] = insert["SoC"]
                d["TotalCapacity"] = insert["TotalCapacity"]
        except Exception as e:
            print "Invalid data: "+str(e)
        self.lock.release()

    def query_started(self, unitid):
        '''
        Called when the query is started. 
        This creates the entry for the unit ID and sets query start timestamp.

        Parameters:
        - unitid: The ID of the unit where the query was sent to.
        '''

        self.lock.acquire()
        self.data[unitid] = {}
        self.data[unitid]['QueryTime'] = time.time()
        self.lock.release()

    def get_all_data(self):
        '''
        Get a copy of all the data in storage.
        
        Returns: The copy of the data in the storage in a dictionary.
        '''

        self.lock.acquire()
        return_data = copy.deepcopy(self.data)
        self.lock.release()
        return return_data

if __name__ == '__main__':
    # Read the aggregator.ini file
    config = ConfigParser.ConfigParser()
    config.read('aggregator.ini')
    hostname = config.get('Connection','hostname')

    # Check that we can parse all the values.
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

    # Initialize the data storage.
    datastorage = DataStorage()

    # Initialize and start the scatter, gather and analyze/post threads.
    scatterer = Scatterer(hostname, unitids, pollinterval, datastorage)
    gatherer = Gatherer(hostname, datastorage)
    analyzerposter = AnalyzerPoster(hostname, datastorage, resultsinterval, unittimeout, minimumsoc, maximumsoc)

    # Run until something kills the script.
    try:
        while True:
            pass
    except:
        pass

    # Kill all the threads.
    scatterer.kill()
    gatherer.kill()
    analyzerposter.kill()
