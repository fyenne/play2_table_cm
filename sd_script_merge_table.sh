#!/bin/bash
# pyspark python3 py2 
export PYSPARK_PYTHON="/app/anaconda3/bin/python"
export PYSPARK_DRIVER_PYTHON="/app/anaconda3/bin/python"
spark-submit --master yarn --deploy-mode client --driver-memory 8G --executor-memory 8G  --conf spark.dynamicAllocation.enabled=true --conf spark.dynamicAllocation.minExecutors=5 --conf spark.dynamicAllocation.maxExecutors=30 --conf spark.dynamicAllocation.initialExecutors=8 --conf spark.shuffle.service.enabled=true --queue root.dsc ./sd_script_merge_table.py --tables  ${tables} 