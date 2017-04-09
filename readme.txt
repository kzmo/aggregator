A Python script that implements an aggregator for a scatter-gather pattern collecting state of charge 
information from units.

Files:
aggregator.py: The aggregator script. Implements three threads and a datastorage class.
aggregator.ini: The configuration file for the aggregator.
test.py: A simple test system to test that the aggregator.py is working.
readme.txt: This file.

Requirements:
- Python 2.7.x. Tested with Python 2.7.3.
- pika Python module. Can be installed with PIP.
- A RabbitMQ server.

To run the test setup:
1) Start aggregator.py with "python aggregator.py" in one shell.
2) Start the testing script test.py in another shell. 
   For example: "python test.py --active 50 --oob 20 --nonresponding 20". 
   On default settings this should create 5 active nodes of which one is out of bounds and 5 inactive nodes of which 
   one is not responding at all.
3) On default settings the shell running the test.py should show results within 30 seconds.

Copyright 2017 Janne Valtanen
