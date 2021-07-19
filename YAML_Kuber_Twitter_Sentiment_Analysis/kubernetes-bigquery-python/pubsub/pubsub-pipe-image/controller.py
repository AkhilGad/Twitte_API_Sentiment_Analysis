"""A simple 'controller' script that determines which app script to run based
on an environment variable.
"""

import os

script = os.environ['PROCESSINGSCRIPT']

if script == 'pubsub-to-bigquery':
    os.system("python pubsub-to-bigquery.py")
elif script == 'twitter-to-pubsub':
    os.system("python twitter-to-pubsub.py")
else:
    print ("unknown script %s" % script)
