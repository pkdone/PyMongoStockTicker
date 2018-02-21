#!/usr/bin/env python
##
# A Python application demonstrating MongoDB's Change Streams capability, by
# simulating a simple "Stock Prices" system Changes to stock prices are
# listened for and displayed to the user, as and when these price changes are
# persisted in the MongoDB database.
#
# Usage (first ensure 'py' script is executable):
#
#   $ ./mongo_stock_ticker.py <COMMAND>
#
# Commands:
# - CLEAN   - Clean out old version of DB & Collection (if exists)
# - INIT    - Initialise DB Collection with ~20k symbols & random prices
# - LISTEN  - Continuously listen for DB Collection changes and print them*
# - UPDATES - Continuously perform random updates to DB Collection*
#
# *Must run 'INIT' first
#
# The MongoDB database/collection created & used is 'market.stocksymbols'
#
# Prerequisites:
#
# 1) Configure and run one of the following:
# * MongoDB Replica Set (e.g. https://github.com/pkdone/mongo-quick-repset)
#  or
# * Sharded Cluster (e.g. https://github.com/pkdone/mongo-multi-svr-generator)
#
# 2) Install PyMongo driver, eg:
#   $ sudo pip install pymongo
#
# 3) Change the value of the MONGODB_URL variable, below, to reflect cluster
#    address
##
import os
import sys
import random
import time
from datetime import datetime
from pymongo import MongoClient


#
# MongoDB cluster connection URL
#
# Connecting to Sharded cluster's 2 mongos processes example
# MONGODB_URL = 'mongodb://localhost:37300,localhost:37301/'
# Connecting to Replica Set example
MONGODB_URL = 'mongodb://localhost:27000,localhost:27001,localhost:27002/?' \
              'replicaSet=TestRS'


####
# Main start function
####
def do_main():
    print(' ')

    if len(sys.argv) < 2:
        print('Error: No command argument provided')
        print_usage()
    else:
        command = sys.argv[1].strip().upper()
        COMMANDS.get(command, print_commands_error)(command)


####
# Initialise MongoDB database collection with records
####
def do_init(*args):
    if stocks_coll().find_one() is not None:
        print('-- Initialisation of collection "%s.%s" not performed because\n'
              '   collection already exists (run with command "CLEAN" first)\n'
              % (DB, COLL))
        return

    enable_collection_sharding_if_required()
    print('-- Initialising collection "%s.%s" with stock price values (this\n'
          '   will take a few minutes)\n' % (DB, COLL))

    for i in xrange(10000, 15000):
        price = random.randrange(10, 20)
        stocks_coll().insert({'_id': 'A%d' % i, 'price': price})
        stocks_coll().insert({'_id': 'K%d' % i, 'price': price})
        stocks_coll().insert({'_id': 'S%d' % i, 'price': price})
        stocks_coll().insert({'_id': 'Z%d' % i, 'price': price})

        if (i % 50) == 0:
                sys.stdout.write('.')
                sys.stdout.flush()

    print('\n')

    for key, price in SYMBOLS.items():
        stocks_coll().insert({'_id': key, 'price': rand_stock_val(key)})


####
# Listen for database collection change events and print out the change summary
#
# Note: In a future version, if updating existing values, probably need to get
# resume token first, before querying 'initial' values, before running watch
# with the explicit resume token provided.
####
def do_listen(*args):
    if stocks_coll().find_one() is None:
        print('-- Listen for changes on collection "%s.%s" aborted because\n'
              '   collection does not exist (run with command "INIT" first)\n'
              % (DB, COLL))
        return

    print('-- Continuously listening & displaying any updates that occur on\n'
          '   key stock symbols in collection "%s.%s"\n' % (DB, COLL))

    cursor = stocks_coll().watch([
        {'$match': {
            'operationType': 'update',
            'updateDescription.updatedFields.price': {'$exists': True},
            'documentKey._id': {'$in': list(SYMBOLS.keys())},
        }}
    ])

    try:
        for doc in cursor:
            print('Stock %s \ttick: %d \t time:%s' % (
                  doc['documentKey']['_id'],
                  doc['updateDescription']['updatedFields']['price'],
                  str(datetime.now().strftime('%H:%M:%S.%f')[:-2])))
    except KeyboardInterrupt:
        keyboard_shutdown()


####
# Loop continuously, each time updating a few records and then sleeping a
# little. Performs approximately 16 operations per second, of which only 4
# operations relate to updates prices to the 'important' stock symbols.
####
def do_updates(*args):
    if stocks_coll().find_one() is None:
        print('-- Performing random updates to collection "%s.%s" aborted\n'
              '   because collection does not exist (run with command "INIT" '
              'first)\n' % (DB, COLL))
        return

    print('-- Continuously performing random updates on key stock symbols\n'
          '   in collection "%s.%s"\n' % (DB, COLL))

    try:
        while True:
            key = random.choice(SYMBOLS.keys())
            stocks_coll().update_one({'_id': key},
                                     {'$set': {'price': rand_stock_val(key)}})
            key = random.randrange(10000, 15000)
            stocks_coll().update_one({'_id': key},
                                     {'$set': {'price': rand_stock_val(key)}})
            key = random.randrange(10000, 15000)
            stocks_coll().delete_one({'_id': 'K%d' % key})
            stocks_coll().insert({'_id': 'K%d' % key,
                                  'price': rand_stock_val(key)})
            time.sleep(0.25)
    except KeyboardInterrupt:
        keyboard_shutdown()


####
# Remove the database collection and its documents
####
def do_clean(*args):
    print('-- Dropping collection "%s.%s" and all its documents\n'
          % (DB, COLL))
    MongoClient(MONGODB_URL).drop_database(DB)


####
# Print out how to use this script
####
def print_usage():
    print('\nUsage:')
    print('\n $ ./mongo_stock_ticker.py <COMMAND>')
    print('\n Command options:')

    for key, price in COMMANDS.items():
        print(' * %s' % key)


####
# Print script startup error reason
####
def print_commands_error(command):
    print('Error: Illegal command argument provided: "%s"' % command)
    print_usage()


####
# Get handle on database.collection
####
def stocks_coll():
    return MongoClient(MONGODB_URL)[DB][COLL]


####
# Return random integer value for a stock symbol between 20 & 89, unless
# symbol is for MongoDB, in which case return random value between 90 & 99
####
def rand_stock_val(symbol):
    return (random.randrange(90, 100) if (symbol == 'MDB')
            else random.randrange(20, 90))


####
# If the target cluster is sharded ensure, shard the database.collection on
# just the '_id' field (not usually recommended but for demos this is fine)
####
def enable_collection_sharding_if_required():
    admin_db = MongoClient(MONGODB_URL).admin

    if admin_db.command('serverStatus')['process'] == 'mongos':
        admin_db.command('enableSharding', DB)
        admin_db.command('shardCollection', '%s.%s' % (DB, COLL),
                         key={'_id': 1})


####
# Swallow the verbiage that is spat out when using 'Ctrl-C' to kill the script
# and instead just print a simple single line message
####
def keyboard_shutdown():
    print('\nInterrupted\n')

    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)


# Constants
DB = 'market'
COLL = 'stocksymbols'
SYMBOLS = {
    'MDB':   'MongoDB Inc.',
    'MULE':  'MuleSoft Inc.',
    'ORCL':  'Oracle Corp.',
    'IBM':   'International Business Machines Corp.',
    'SAP':   'SAP SE',
    'ADBE':  'Adobe Systems Inc.',
    'AMZN':  'Amazon.com Inc.',
    'MSFT':  'Microsoft Corp.',
    'CSCO':  'Cisco Systems Inc.',
    'VMW':   'VMware Inc.',
    'AAPL':  'Apple Inc.',
    'GOOGL': 'Alphabet Inc.',
    'FB':    'Facebook, Inc.',
}

COMMANDS = {
    'INIT':    do_init,
    'LISTEN':  do_listen,
    'UPDATES': do_updates,
    'CLEAN':   do_clean,
}


####
# Main
####
if __name__ == '__main__':
    do_main()
