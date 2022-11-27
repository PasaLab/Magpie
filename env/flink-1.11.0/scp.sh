cd conf
scp  flink-conf.yaml experiment@slave034:`pwd`
scp  flink-conf.yaml experiment@slave035:`pwd`
scp  flink-conf.yaml experiment@slave036:`pwd`
cd ..
./bin/stop-cluster.sh
sleep 15s
./bin/start-cluster.sh
