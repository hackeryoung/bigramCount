docker build .

./start-master.sh
./start-worker.sh 

docker cp text.txt spark_master:/root/
docker cp bigramCount.py spark_master:/root/

docker exec -d spark_master sh -c "cd /root/src; /root/src/run.sh"