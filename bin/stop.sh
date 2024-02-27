#!/bin/bash
NAME="ParameterOptimization"    # the process to kill
echo $NAME
ID=`ps -ef | grep "$NAME" | grep -v "grep" | awk '{print $2}'`  #注意此shell脚本的名称，避免自杀
if [ -z "$ID" ];then
    echo "process id is empty, process is not existed..."
else
    echo $ID
        for id in $ID
        do
        kill -9 $id
        echo "killed $id"
    done
fi


