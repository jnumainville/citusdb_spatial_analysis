# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import psycopg2
import subprocess


def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    
    connection = psycopg2.connect(database=theConnectionDict['db'], user=theConnectionDict['user'])

    return connection


def CreateReferenceTable(pgCon, tableName):
    """
    SELECT create_reference_table('states_reference_table');
    """
    pass


def CreateDistributedTable(pgCon, tableName, hashFieldName):
    """
    SELECT create_distributed_table('states_hash3' , 'hash_3');
    """
    pass

def SetShardCount(pgCon):
    """
    #ALTER DATABASE research SET citus.shardcount = 12
    """

def GetIndexNames(pgCon,tableName):
    """
    From https://stackoverflow.com/questions/2204058/list-columns-with-indexes-in-postgresql
    
    SELECT indexname, tablename, schemaname FROM pg_indexes WHERE tablename = 'counties';
       
    """
    pass

def DropIndex(pgCon, indexName):
    """
    
    """
    pass

def LoadShapefile(shapeFilePath, pgTableName, srid=4326):
    """
    
    shp2pgsql -s 4326 /data/projects/G-818404/vector_datasets/randpoints_50m.shp random_points_50million | psql -p 9700 -U dhaynes research
    
    Not computing a spatial index. No indices are necessary before distributing the data
    
    """

def CreateGeoIndex(pgCon, tableName, indexName):
    """
    CREATE INDEX clicked_at_idx ON tableName USING GIST(geom);
    """
    pass

def CreateBTreeIndex(pgCon, tableName, indexName, fieldName):
    """
    CREATE INDEX clicked_at_idx ON tableName ([fieldName]);
    
    fieldName is a list
    """
    if len(fieldName) > 1: fieldName = ",".join(fieldName)
    pass    
    