# -*- coding: utf-8 -*-
"""
Spyder Editor

This script/module is used to load data into CitusDB / PostgreSQL
"""

import psycopg2, timeit
import subprocess


def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    
    connection = psycopg2.connect(database=theConnectionDict['db'], user=theConnectionDict['user'], port=theConnectionDict['port'])

    return connection

def ExecuteQuery(pgCon, query):
    
    pgCur = pgCon.cursor()
    try:
        pgCur.execute(query)
    except:
        print(query)
    
    return pgCur

def CreateReferenceTable(tableName):
    """
    SELECT create_reference_table('states_reference_table');
    """
    
    return "SELECT create_reference_table('{}')".format(tableName)


def CreateDistributedTable(tableName, hashFieldName):
    """
    SELECT create_distributed_table('states_hash3' , 'hash_3');
    """
    return "SELECT create_distributed_table('{}' , '{}')".format(tableName, hashFieldName)

def SetShardCount(databaseName, shardCount):
    """
    ALTER DATABASE research SET citus.shardcount = 12
    """
    
    return "ALTER DATABASE {} SET citus.shardcount = {}".format(databaseName, shardCount)
    

def GetIndexNames(tableName):
    """
    From https://stackoverflow.com/questions/2204058/list-columns-with-indexes-in-postgresql
    
    SELECT indexname, tablename, schemaname FROM pg_indexes WHERE tablename = 'counties';
    
    This needs to be fed into a query with results.
       
    """
    return "SELECT indexname, tablename, schemaname FROM pg_indexes WHERE tablename = '{}';".format(tableName)

def DropIndex(indexName):
    """
    
    """
    return "DROP INDEX {};".format(indexName)

def LoadShapefile(shapeFilePath, pgTableName, connectionDict, srid=4326):
    """
    
    shp2pgsql -s 4326 /data/projects/G-818404/vector_datasets/randpoints_50m.shp random_points_50million | psql -p 9700 -U dhaynes research
    
    Not computing a spatial index. No indices are necessary before distributing the data
    
    """
    
    shp2psqlCommand = """shp2pgsql -s {} {} {}""".format(srid, shapeFilePath, pgTableName)
    toDBCommand = """psql -p {port} -U {user} -d {db} """.format(**connectionDict)
    
    
    finalCommand = r"%s | %s" % (shp2psqlCommand, toDBCommand)
    print(finalCommand)
    
    p = subprocess.Popen(finalCommand, stdout=subprocess.PIPE, shell=True)
    p.wait()
    out, err = p.communicate()
    
    return (shapeFilePath, pgTableName)


def CreateGeoIndex(tableName, indexName, geomField="geom"):
    """
    CREATE INDEX clicked_at_idx ON tableName USING GIST(geom);
    """
    return "CREATE INDEX {} ON {} USING GIST({});".format(indexName, tableName, geomField)

def CreateBTreeIndex(tableName, indexName, fieldName):
    """
    CREATE INDEX clicked_at_idx ON tableName ([fieldName]);
    
    fieldName is a list
    """
    if len(fieldName) > 1: fieldName = ",".join(fieldName)
    pass    



myConnection = {"host": "localhost", "db": "research", "port": 5432, "user": "david"}

LoadShapefile("E:\scidb\counties.shp", "counties_us", myConnection, 4326)

#myConn = CreateConnection(myConnection)
#myCursor = myConn.cursor()
    