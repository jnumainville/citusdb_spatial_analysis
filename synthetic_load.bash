#!/bin/bash
#Script for batch loading data into SciDB

while getopts v: option
do
    case "${option}"
    in
    v) vector=${OPTARG};;
    esac
done


for d in 1 10 50 100;
do
	for h in 2 4 6 8;
        do
		#echo "${chunk}" and "${tiles}"
		args=("--distributed" "--host localhost" "-d postgres" "-p 9700" "-u dhaynes" "-o /group/vector_datasets/postgresql_loading_${vector}_${d}_hash_${h}_d7_13.csv" "-s 4326" "-t synthetic_${d}_hash_${h}" "-f hash_${h}" "-n 12" "csv" "--txt /group/vector_datasets/geom_csv/synthetic_households_${d}per_hashed.csv" "--geom geom_text" "--keyvalue gid=bigint" "--keyvalue geom_text=text" "--keyvalue hash_8=text" "--keyvalue hash_6=text" "--keyvalue hash_4=text" "--keyvalue hash_2=text") 
		python3 citus_parallel_load.py  ${args[@]}
		#echo ${args[@]}
	done
done
