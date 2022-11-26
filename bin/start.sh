#!/bin/sh


bin="$(cd "$( dirname "$0")";pwd)"
magpie_home=$(dirname "${bin}")
java="${JAVA_HOME}/bin/java"
main_class="ParameterOptimization"
magpie_jar="${magpie_home}/target/Magpie-1.0-SNAPSHOT-jar-with-dependencies.jar"
magpie_conf="${magpie_home}/conf"
java_opts+=" -Dmagpie.dir=${magpie_home}"
java_opts+=" -Dmagpie.conf.dir=${magpie_conf}"
magpie_log="${magpie_home}/logs"

if [ ! -d ${magpie_log} ]; then
  mkdir ${magpie_log}
fi

nohup ${java} ${java_opts} -cp ${magpie_jar} ${main_class} >> ${magpie_log}/task.out 2>&1 &

