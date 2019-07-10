# -*- coding: utf-8 -*-
"""
Spyder Editor

This script/module is used to for performance tests on CitusDB (Vector)
"""

import psycopg2, timeit, csv
import subprocess
from psycopg2 import extras
from collections import OrderedDict



def CreateConnection(theConnectionDict):
    """
    This method will get a connection. Need to make sure that the DB is set correctly.
    """
    
    connection = psycopg2.connect(host=theConnectionDict['host'], database=theConnectionDict['db'], user=theConnectionDict['user'], port=theConnectionDict['port'])

    return connection

def ExecuteQuery(pgCon, query):
    
    pgCur = extras.DictCursor(pgCon)
    try:
        pgCur.execute(query)
    except:
        print("ERROR...", query)
    
    return pgCur

def PointPolygonQuery(datasets):
    """
    
    """
    completeQuery = []
    for d in datasets:
        query = """ SELECT b.gid, count(p.gid) as num_features FROM {point_table} p INNER JOIN {poly_table} b ON ST_WITHIN(p.geom, b.geom) GROUP BY b.gid""".format(**d)
        #b.{point_column_name},
        completeQuery.append(query)
        
    return completeQuery


def PointPolygonJoin(pointDatasets, polygonDatasets):
    """
    This function generates the datasets needed for perfoming point in polygon spatial join
    """
    #OrderedDict( [( ("point", "%s_hash_%s" % (p, h)), ("polygon", poly) ) for p in pointDatasets for h in range(2,10,2) for poly in polygonDatasets ] )
    return [ OrderedDict([ ("point_table", "%s_hash_%s" % (p, h)), ("poly_table", poly) ]) for p in pointDatasets for h in range(2,10,2) for poly in polygonDatasets ]

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

    parser = argparse.ArgumentParser(description= "Analysis Script for running Spatial Analytics on CitusDB")  
    parser.add_argument("-csv", required =False, help="Output timing results into CSV file", dest="csv", default=None)  

    parser.add_argument("-r", required =False, type=int, help="Number of runs", dest="runs", default=3)  
   
    #All of the required connection information
    parser.add_argument("--host", required=True, type=str, help="Host of database", dest="host")
    parser.add_argument("-d", required=True, type=str, help="Name of database", dest="db")
    parser.add_argument("-p", required=True, type=int, help="port number of citusDB", dest="port")   
    parser.add_argument("-u", required=True, type=str, help="db username", dest="user")

    subparser = parser.add_subparsers(dest="command")
    pointPolyJoin_subparser = subparser.add_parser('point_polygon_join')
    pointPolyJoin_subparser.set_defaults(func=PointPolygonJoin) #["random","synthetic"])
    
    pointPolyJoin_subparser.add_argument("--point", required=False, help="Table name of the point dataset", dest="point")
    pointPolyJoin_subparser.add_argument("--polygon", required=False, help="Table name of the polygon dataset", dest="polygon")


    # count_subparser = subparser.add_parser('count')
    # count_subparser.set_defaults(func=localDatasetPrep)
    
    # reclass_subparser = subparser.add_parser('reclassify')
    # reclass_subparser.set_defaults(func=localDatasetPrep)

    # focal_subparser = subparser.add_parser('focal')
    # focal_subparser.set_defaults(func=localDatasetPrep)

    # overlap_subparser = subparser.add_parser('overlap')
    # overlap_subparser.set_defaults(func=localDatasetPrep)

    # add_subparser = subparser.add_parser('add')
    # add_subparser.set_defaults(func=localDatasetPrep)

    return parser


if __name__ == '__main__':

    args, unknown = argument_parser().parse_known_args()
    # print(args)
    
    myConnection = {"host": args.host, "db": args.db, "port": args.port, "user": args.user}
    # #myConnection = {"host": "localhost", "db": "research", "port": args.port, "user": "david"}
    
    
    psqlCon = CreateConnection(myConnection)
    timings = OrderedDict()
    
    
    if args.command == "point_polygon_join":
        
        pointDatasets = ["random", "synthetic"]
        polygonDatasets = ["state", "county", "tracts", "blocks"]
        datasets = args.func(pointDatasets, polygonDatasets)
        queries = PointPolygonQuery(datasets)
        print(queries)    

    
    for query, d in zip(queries, datasets):
        for r in range(1,args.runs+1):
            start = timeit.default_timer()
            #ExecuteQuery(psqlCon, query)
            stop = timeit.default_timer()
            queryTime = stop-start
            # tables = "%s_%s" % ()
            timings[(r,d["point_table"], d["poly_table"])] = OrderedDict([ ("point_table", d["point_table"]), ("poly_table", d["poly_table"]), ("query_time", queryTime)  ])

    print(timings)
    if args.csv: WriteFile(args.csv, timings)
    print("Finished")