echo "Building Spark Docker Image..."
docker build .

echo "Initializing Master Node..."
sh start-master.sh

sleep 3
echo "Initializing Worker Node..."
sh start-worker.sh

echo "Running BigramCount..."
docker exec -d spark_master sh -c "cd /root/src; /root/src/run.sh"

sleep 30
echo "BigramCount finishes"
