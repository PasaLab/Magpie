# Magpie
Efficient Big Data Query System Parameter Optimization based on Pre-selection and Search Pruning Approach

Magpie
===============

This is the code repository for the a big data system parameter automatic optimization paper titled 'Magpie: Efficient Big Data Query System Parameter Optimization based on Pre-selection and Search Pruning Approach'.

Magpie can recommend the best parameter configuration of the big data system (Flink,Spark,etc.)according to the performance target requirements and parameters set by the user and their range of values.

System Environment 
---------

        CentOS 7.5
        Java 1.8
        Python 3.6.3
        Hadoop 2.6.7
        Hive 2.3.4
        Flink 1.11.0
        Prometheus 2.19.2
        Pushgateway 1.2.0

When installing java, hadoop, hive and Flink, please make sure to set user environment variables for them, such as `JAVA_HOME`, `HADOOP_HOME`, `FLINK_HOME` and `PATH`

Before the system is running, use Python to load the LightGBM dependency package, install the command:` pip install lightgbm`

Before the system runs, please make sure that your job can run normally in the Flink cluster

System Operation
----------

1. Compile and package

       cd Magpie
       mvn clean install -Dmaven.test.skip=true

2. System configuration: configure flink parameters and values, inspected performance indicators, performance goals, flink execution jobs and job types and other parameters in `conf/config.yaml`

       #Flink dir
       flink.dir: /home/experiment/wj2/env/flink-1.11.0
       #Flink parameters values
       parameters:
           taskmanager.memory.process.size: [2g,3g,4g,5g,6g,7g,8g,9g,40g,12g,14g,16g,18g,20g,24g,30g]
           taskmanager.numberOfTaskSlots: [2,3,4,5,6,7,8,9,10,11,12,16,20]
           taskmanager.memory.network.fraction: [0.05,0.1,0.15,0.2, 0.25]     
           taskmanager.memory.managed.fraction: [0.2,0.25,0.3,0.35,0.4,0.45,0.5,0.6,0.7]
            parallelism.default: [2,4,8,10,16,20,30,32,40,48,50,60,70,80]
       #performance target
       target: 1.0
       #Flink Job compute model
       flink.job.model: batch
       #job type
       flink.job.type: SQL
       #Flink job submit
       job.submit.cmd: ./bin/flink  run -m yarn-cluster  -c  org.apache.flink.benchmark.Benchmark\  
               ~/wj2/project/flink-tpcds/target/flink-tpcds-0.1-SNAPSHOT-jar-with-dependencies.jar\    
       		--database tpcds_bin_orc_100\ 
               --hive_conf   /home/experiment/wj2/env/apache-hive-2.3.4-bin/conf\ 
               --queries q7.sql

3. Running

       ./bin/start.sh &

   After the system is running, you can check whether the Flink job is running normally on `Flink Web port 8081` or `Yarn port 8088`, and you can check job performance data on `Prometheus Web port 9091`. If you want to stop the system running, execute the command `./bin/stop.sh`

4. Operation result: monitor the parameter search process and view the recommended configuration parameter result output

       tail –f logs/task.log （Running）
       tail –f logs/task.out （After running）

