#!/bin/bash
#Script for loading all CSV data into SciDB

for vector in "states" "counties" "tracts" "blocks";
do
		args=(
      "--reference"
      "--host localhost"
      "-d postgres"
      "-p 9700"
      "-u dhaynes"
      "-o /group/vector_datasets/postgresql_loading_${vector}_7_12.csv"
      "-s 4326"
      "-t ${vector}"
      "csv"
      "--txt /group/vector_datasets/geom_csv/${vector}.csv"
      "--geom geom_text"
		)
		python3 citus_parallel_load.py  "${args[@]}"
done
