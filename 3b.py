"""
CS61 Lab 3b: MongoDB Blog Stream
Matthew Parker and Dominic Carrese

19W

"""

from configparser import ConfigParser
import pymongo
from pymongo.errors import ConnectionFailure
import sys
from pprint import pprint


"""
Read the configuration file for DB connection
Returns the .ini credentials as a dictionary 
"""
def config(filename = 'Team13Lab3.ini', section = 'mongo'):
    parser = ConfigParser();
    parser.read(filename)

    credentials = {}
    if parser.has_section(section):
        for item in parser.items(section):
            credentials[item[0]] = item[1]
    else:
        raise Exception('section {0} not found in {1}!'.format(section,filename))

    return credentials


"""
Connect to Atlas using credentials from .ini file
returns the connection and the collection

"""
def connect(credentials):
    team = credentials["team"]
    password = credentials["password"]
    dbname = credentials["dbname"]
    server = "mongodb://" + team + ":" + password + "@cluster0-shard-00-00-ppp7l.mongodb.net:27017,cluster0-shard-00-01-ppp7l.mongodb.net:27017,cluster0-shard-00-02-ppp7l.mongodb.net:27017/" + dbname + "?ssl=true&replicaSet=Cluster0-shard-0&authSource=admin"

    try:
        connection = pymongo.MongoClient(server)
        db = connection.Team13DB.test
        print('Connected')
        return db,connection

    except ConnectionFailure:
        print('Connection Failed')
        sys.exit(1)

"""
fetch one element from test collection (added manually from terminal)
"""
def test_fetch(db):
    pprint(db.find_one())


"""
Closes connection to DB
"""
def clean_up(connection):
    connection.close()
    print('Disconnected')




if __name__ == '__main__':
    credentials = config()
    db,connection = connect(credentials)
    test_fetch(db)
    clean_up(connection)





