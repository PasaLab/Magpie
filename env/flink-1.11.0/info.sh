#!/bin/sh 
Folder_A="/home/experiment/wj2/project/flink-sql-benchmark-master/flink-tpcds_2020_12_09/src/main/resources/queries"  
for file_a in ${Folder_A}/*
do  
    temp_file=`basename $file_a`  
    echo $temp_file >> sqlDAG
    ./bin/flink info  -c  org.apache.flink.benchmark.Benchmark  /home/experiment/wj2/project/flink-sql-benchmark-master/flink-tpcds_2020_12_09/target/flink-tpcds-0.1-SNAPSHOT-jar-with-dependencies.jar --database tpcds_bin_orc_10 --hive_conf   /home/experiment/wj2/env/apache-hive-2.3.4-bin/conf  --location /home/experiment/wj2/project/flink-sql-benchmark-master/flink-tpcds_2020_12_09/src/main/resources/queries/$temp_file >> sqlDAG
   sleep 100s
  
done

