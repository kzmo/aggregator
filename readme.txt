A Python script that implements an aggregator for a scatter-gather pattern collecting state of charge information from units.

Files:
aggregator.py: The aggregator script. Implements three threads and a datastorage class.
aggregator.ini: The configuration file for the aggregator.
test.py: A simple test system to test that the aggregator.py is working.
readme.txt: This file.

Requirements:
- Python 2.7.x. Tested with Python 2.7.3.
- pika Python module. Can be installed with PIP.
- A RabbitMQ server.

Copyright 2017 Janne Valtanen
