# -*- coding: utf-8 -*-
"""
Spyder Editor

This script/module is used to load data into CitusDB / PostgreSQL
"""

import psycopg2, timeit, csv
import subprocess
from psycopg2 import extras
from collections import OrderedDict


def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    
    connection = psycopg2.connect(host=theConnectionDict['host'], database=theConnectionDict['db'], user=theConnectionDict['user'], port=theConnectionDict['port'], password='haynes')

    return connection

def ExecuteQuery(pgCon, query):
    
    pgCur = extras.DictCursor(pgCon)
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

def PartitionTable(pgCon, tableName, hashFieldName, setShardPartitions):
    """
    Metafunction that does a few operations
    
    """
    pgCur = ExecuteQuery(pgCon, setShardPartitions)
    pgCur = ExecuteQuery(pgCon, CreateDistributedTable(tableName, hashFieldName.lower()))
    del pgCur
    
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
    
    p = subprocess.Popen(finalCommand, shell=True)
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
    if len(fieldName) > 1 and isinstance(fieldName, str): 
        fieldName = ",".join(fieldName)
    elif len(fieldName) == 1 and isinstance(fieldName, list):
        fieldName = fieldName[0]
    else:
        print("ERROR %s" % (fieldName))
    
    return "CREATE INDEX {} ON {} USING BTREE({});".format(indexName, tableName, fieldName)

def CreateIndices(pgCon, indices):
    """
    
    """
    for i in indices:
        print(i)
        pgCur = ExecuteQuery(pgCon, i)
        pgCur.close()
    

def RemoveIndices(pgCon, pgTable):
    """
    Remove indices
    """
    
    pgCur = ExecuteQuery(pgCon, GetIndexNames(pgTable))
    for r in pgCur:
        #print(r['indexname'])
        dropIndexQuery = DropIndex(r['indexname'])
        print(dropIndexQuery)
        ExecuteQuery(pgCon, dropIndexQuery)

def WriteFile(filePath, theDictionary):
    """
    This function writes out the dictionary as csv
    """
    
    thekeys = list(theDictionary.keys())
    
    with open(filePath, 'w') as csvFile:
        fields = list(theDictionary[thekeys[0]].keys())
        theWriter = csv.DictWriter(csvFile, fieldnames=fields)
        theWriter.writeheader()

        for k in theDictionary.keys():
            theWriter.writerow(theDictionary[k])        

def argument_parser():
    """
    Parse arguments and return Arguments
    """
    import argparse

    parser = argparse.ArgumentParser(description= "Module for loading data into CitusDB")    
    
    parser.add_argument("-shp", required=True, help="Input file path for the shapefile", dest="shapefilePath")    
    parser.add_argument("-s", required=True, help="Input SRID number", dest="srid")    
    
    parser.add_argument("-t", required=True, type=str, help="Name of CituSDB table", dest="tableName")
    parser.add_argument("-f", required=False, type=str, help="Field Name for sharded table", dest="shardKey")
    parser.add_argument("-n", required=False, type=str, help="Number of partitions", dest="partitions")
    #All of the required connection informaiton
    parser.add_argument("-d", required=True, type=str, help="Name of database", dest="db")
    parser.add_argument("-host", required=True, type=str, help="Host of database", dest="host")
    parser.add_argument("-p", required=True, type=int, help="port number of citusDB", dest="port")   
    parser.add_argument("-u", required=True, type=str, help="db username", dest="user")
    
    parser.add_argument("-o", required=False, type=str, help="The file path of the csv", dest="csv", default=None)

    return parser
        

if __name__ == '__main__':
    args = argument_parser().parse_args()
    myConnection = {"host": args.host, "db": args.db, "port": args.port, "user": args.user}
    #myConnection = {"host": "localhost", "db": "research", "port": args.port, "user": "david"}
    start = timeit.default_timer()
    LoadShapefile(args.shapefilePath, args.tableName, myConnection, args.srid)
    stopLoadShapefile = timeit.default_timer()
    psqlCon = CreateConnection(myConnection)
    RemoveIndices(psqlCon, args.tableName)
    psqlCon.commit()
    stopRemoveIndices = timeit.default_timer()
    shardQuery = SetShardCount(args.db, args.partitions)
    PartitionTable(psqlCon, args.tableName, args.shardKey, shardQuery)
    psqlCon.commit()
    stopPartitionTable = timeit.default_timer()
    geoIndex = CreateGeoIndex(args.tableName, "{}_geom_gist".format(args.tableName, ) )
    bTreeIndex = CreateBTreeIndex(args.tableName, "{}_{}_btree".format(args.tableName, args.shardKey), [args.shardKey])
    #CreateIndices(psqlCon, [geoIndex, bTreeIndex] )
    stopCreateIndices = timeit.default_timer()
    psqlCon.commit()
    
    times = OrderedDict( [("connectionInfo", "XSEDE"), ("dataset", args.tableName), ("shapefile", args.shapefilePath), ("full_time", stopCreateIndices-start), \
        ("load_time", stopLoadShapefile-start), ("remove_indices", stopRemoveIndices-stopLoadShapefile), ("partition_time", stopPartitionTable-stopRemoveIndices),\
        ("create_distributed_indices", stopCreateIndices)])
    
    print("All Processes have been completed: {:.2f} seconds".format(stopCreateIndices-start))
    print(times) 
    if args.csv:
        WriteFile(args.csv, times)

    print(times)
    
    psqlCon.close()

    
