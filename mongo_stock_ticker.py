#!/usr/bin/env python
##
# A Python application demonstrating MongoDB's Change Streams capability, by
# simulating a simple "Stock Prices" system. Changes to stock prices are
# listened for and displayed to the user, as and when these price changes are
# persisted in the MongoDB database.
#
# Usage (first ensure 'py' script is executable):
#
#   $ ./mongo_stock_ticker.py <COMMAND>
#
# Commands:
# - CLEAN    - Clean out old version of DB & Collection (if exists)
# - INIT     - Initialise DB Collection with ~20k symbols & random prices
# - CHANGE   - Continuously perform random changes to records in DB Collection*
# - TRACE    - Continuously listen for DB Collection changes & print them*
# - DISPLAY  - Continuously listen for DB Collection changes & display each
#              price change in the console next to its respective stock symbol*
#
# *Must run 'INIT' first
#
# The MongoDB database/collection created & used is 'market.stocksymbols'
#
# Prerequisites:
#
# 1) Configure and run one of following using MongoDB (version 3.6+):
# * MongoDB Replica Set (e.g. https://github.com/pkdone/mongo-quick-repset)
#  or
# * Sharded Cluster (e.g. https://github.com/pkdone/mongo-multi-svr-generator)
#
# 2) Install PyMongo driver (version 3.6+), eg:
#   $ sudo pip install pymongo
#
# 3) Change the value of the MONGODB_URL variable, below, to reflect the
#    cluster's address
##
import os
import sys
import random
import time
import curses
from pprint import pprint
from curses import wrapper
from datetime import datetime
from string import ascii_uppercase
from pymongo import MongoClient


#
# MongoDB cluster connection URL
#
# Connecting to Sharded cluster's 2 mongos processes example:
# MONGODB_URL = 'mongodb://localhost:37300,localhost:37301/'
# Connecting to Replica Set example:
MONGODB_URL = 'mongodb://localhost:27000,localhost:27001,localhost:27002/?' \
              'replicaSet=TestRS'
mongo_client = MongoClient(MONGODB_URL)


####
# Main start function
####
def main():
    print('')

    if len(sys.argv) < 2:
        print('Error: No command argument provided')
        print_usage()
    else:
        command = sys.argv[1].strip().upper()
        COMMANDS.get(command, print_commands_error)(command)


####
# Initialise MongoDB database collection with a set of records
####
def do_init(*args):
    if stocks_coll().find_one() is not None:
        print('-- Initialisation of collection "%s.%s" not performed because\n'
              '   collection already exists (run with command "CLEAN" first)\n'
              % (DB, COLL))
        return

    print('-- Initialising collection "%s.%s" with stock price values\n'
          % (DB, COLL))

    enable_collection_sharding_if_required()

    for i in xrange(RANDKEY_LOWER, RANDKEY_UPPER):
        price = random.randrange(10, 20)
        stocks_coll().insert({'_id': 'A%d' % i, 'price': price})
        stocks_coll().insert({'_id': 'K%d' % i, 'price': price})
        stocks_coll().insert({'_id': 'S%d' % i, 'price': price})
        stocks_coll().insert({'_id': 'Z%d' % i, 'price': price})

        if (i % 100) == 0:
                sys.stdout.write('.')
                sys.stdout.flush()

    print('\n')

    for key in SYMBOLS.keys():
        stocks_coll().insert({'_id': key, 'price': rand_stock_val(key)})


####
# Remove the database collection and its documents
####
def do_clean(*args):
    print('-- Dropping collection "%s.%s" and all its documents\n'
          % (DB, COLL))
    mongo_client.drop_database(DB)


####
# Loop continuously, each time updating a few records and then sleeping a
# little. Performs approximately 16 operations per second, of which only 4
# operations relate to updating prices of the 'important' stock symbols.
####
def do_change(*args):
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
            # Update random important stock symbol's price
            stocks_coll().update_one({'_id': key},
                                     {'$set': {'price': rand_stock_val(key)}})
            key = random.randrange(RANDKEY_LOWER, RANDKEY_UPPER)
            # Update random arbitrary stock symbol's price
            stocks_coll().update_one({'_id': 'A%d' % key},
                                     {'$set': {'price': rand_stock_val(key)}})
            key = random.randrange(RANDKEY_LOWER, RANDKEY_UPPER)
            # Delete random stock symbol
            stocks_coll().delete_one({'_id': 'Z%d' % key})
            # Insert deleted stock symbol back in
            stocks_coll().insert({'_id': 'Z%d' % key,
                                  'price': rand_stock_val(key)})
            time.sleep(0.25)
    except KeyboardInterrupt:
        keyboard_shutdown()


####
# Listen for database collection change events and print out the changes as
# they occur
####
def do_trace(*args):
    if stocks_coll().find_one() is None:
        print('-- Listen for changes on collection "%s.%s" aborted because\n'
              '   collection does not exist (run with command "INIT" first)\n'
              % (DB, COLL))
        return

    print('-- Continuously listening & displaying any updates that occur on\n'
          '   key stock symbols in collection "%s.%s"\n' % (DB, COLL))

    cursor = stocks_coll().watch(get_stock_watch_filter())

    try:
        for doc in cursor:
            print('Stock %s \ttick: %d \t time: %s' % (
                  doc['documentKey']['_id'],
                  doc['updateDescription']['updatedFields']['price'],
                  str(datetime.now().strftime('%H:%M:%S.%f')[:-2])))
    except KeyboardInterrupt:
        keyboard_shutdown()


####
# Continuously listen for database collection change events and display each
# price change inline in the console, next to its respective stock symbol
####
def do_display(*args):
    if stocks_coll().find_one() is None:
        print('-- Display changes for collection "%s.%s" aborted because\n'
              '   collection does not exist (run with command "INIT" first)\n'
              % (DB, COLL))
        return

    try:
        wrapper(show_console_ui)
    except KeyboardInterrupt:
        keyboard_shutdown()


####
# Show the Console 'ncurses' UI with stock prices constantly changing inline
####
def show_console_ui(stdscr):
    init_console_ui(stdscr)
    symbols_list = SYMBOLS.keys()
    now = datetime.now()
    last_updated_tracker = {}
    (last_price_tracker, resume_token) = get_init_stock_vals_plus_resume_tkn()

    for symbol in symbols_list:
        last_updated_tracker[symbol] = now

    for symbol in symbols_list:
        # Symbol postn: 0, Colon postn: 6, Price postn: 8
        stdscr.addstr(symbols_list.index(symbol), 0, symbol)
        stdscr.addstr(symbols_list.index(symbol), 6, ':')
        stdscr.addstr(symbols_list.index(symbol), 8,
                      str(last_price_tracker[symbol]))

    # Position Ctrl-C text on line below last symbol
    stdscr.addstr(len(symbols_list)+1, 0, '(press "Ctrl-C" to quit)')
    refresh_console_ui(stdscr, len(symbols_list)+2)
    cursor = stocks_coll().watch(get_stock_watch_filter(),
                                 resume_after=resume_token)

    for doc in cursor:
        changed_symbol = doc['documentKey']['_id']
        price = doc['updateDescription']['updatedFields']['price']
        now = datetime.now()
        last_updated_tracker[changed_symbol] = now
        last_price_tracker[changed_symbol] = price

        for symbol in symbols_list:
            # Recently changed symbol prices show highlighted
            if (now - last_updated_tracker[symbol]).total_seconds() < 1:
                stdscr.addstr(symbols_list.index(symbol), 8,
                              str(last_price_tracker[symbol]),
                              curses.A_REVERSE)
            else:
                stdscr.addstr(symbols_list.index(symbol), 8,
                              str(last_price_tracker[symbol]))

        refresh_console_ui(stdscr, len(symbols_list)+2)


####
# Get a resume token for the first change received, then query the values for
# all important stock symbols, returning both pieces of data
####
def get_init_stock_vals_plus_resume_tkn():
    print('\n**Waiting for just one change event on collection "%s.%s", to '
          'obtain a resume token, before being able to track & show changes**'
          % (DB, COLL))

    cursor = stocks_coll().watch()
    doc = next(cursor)  # waits indefinitely if no subsequent changes are made
    resume_token = doc.get("_id")
    last_price_tracker = {}
    cursor = stocks_coll().find({'_id': {'$in': list(SYMBOLS.keys())}})

    for doc in cursor:
        last_price_tracker[doc['_id']] = doc['price']

    return (last_price_tracker, resume_token)


####
# Clear the UI ready to show changing data
####
def init_console_ui(stdscr):
    stdscr.nodelay(True)
    stdscr.clear()


####
# Show any display changes that have occurred, in the UI
####
def refresh_console_ui(stdscr, cursor_row_pos):
    stdscr.addstr(cursor_row_pos, 0, '')
    stdscr.refresh()


####
# Get handle on database.collection
####
def stocks_coll():
    return mongo_client[DB][COLL]


####
# If the target cluster is sharded, shard the database.collection on just the
# '_id' field (not usually recommended but for demos this is fine). Also, pre-
# split the collection's chunks to ensure added records are spread across all
# shards from day one, to avoid Changes Streams issues with sharded clusters
# w.r.t.: (i) 'no resume token' errors, and (ii) gaps of 10 seconds between
# sets of change events.
####
def enable_collection_sharding_if_required():
    admin_db = mongo_client.admin

    if admin_db.command('serverStatus')['process'] == 'mongos':
        admin_db.command('enableSharding', DB)
        admin_db.command('shardCollection', '%s.%s' % (DB, COLL),
                         key={'_id': 1})

        # Create temp docs from A-Z to help with subsequent split of chunks
        for c in ascii_uppercase:
            stocks_coll().insert({'_id': '%s%s%s%s%s' % (c, c, c, c, c),
                                  'price': 0})

        # Split chunks at various intervals (would use 'middle' rather than
        # 'find' but PyMongo doesn't allow this admin command to be invoked)
        for c in ['B', 'C', 'D', 'E', 'F', 'K', 'T', 'U', 'V', 'X', 'Y']:
            admin_db.command({'split': '%s.%s' % (DB, COLL),
                              'find': {'_id': '%s' % c}})

        # Wait 15 seconds to allow the balancer to move chunks across shards
        for i in xrange(0, 30):
                sys.stdout.write('.')
                sys.stdout.flush()
                time.sleep(0.5)

        # Remove temp docs from A-Z as no longer needed
        for c in ascii_uppercase:
            stocks_coll().remove({'_id': '%s%s%s%s%s' % (c, c, c, c, c)})


####
# Return the aggregation filter to be used to watch important stock symbol
# price changes only
####
def get_stock_watch_filter():
    return [
        {'$match': {
            'operationType': 'update',
            'updateDescription.updatedFields.price': {'$exists': True},
            'documentKey._id': {'$in': list(SYMBOLS.keys())},
        }}
    ]


####
# Return random integer value for a stock symbol between 20 & 89, unless
# symbol is for MongoDB, in which case return random value between 90 & 99
####
def rand_stock_val(symbol):
    return (random.randrange(90, 100) if (symbol == 'MDB')
            else random.randrange(20, 90))


####
# Print out how to use this script
####
def print_usage():
    print('\nUsage:')
    print('\n $ ./mongo_stock_ticker.py <COMMAND>')
    print('\n Command options:')

    for key in COMMANDS.keys():
        print(' * %s' % key)

    print(' ')


####
# Print script start-up error reason
####
def print_commands_error(command):
    print('Error: Illegal command argument provided: "%s"' % command)
    print_usage()


####
# Swallow the verbiage that is spat out when using 'Ctrl-C' to kill the script
# and instead just print a simple single line message
####
def keyboard_shutdown():
    print('Interrupted\n')

    try:
        sys.exit(0)
    except SystemExit:
        os._exit(0)


# Constants
DB = 'market'
COLL = 'stocksymbols'
RANDKEY_LOWER = 10000
RANDKEY_UPPER = 15000
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
    'CLEAN':    do_clean,
    'INIT':     do_init,
    'CHANGE':   do_change,
    'TRACE':    do_trace,
    'DISPLAY':  do_display,
}


####
# Main
####
if __name__ == '__main__':
    main()
