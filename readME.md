kafka
1. docker-compose up -d 
2. pipenv install && pipenv shell
3. run kafka script
spark
1. docker build -t cluster-apache-spark:3.4.1 .
2. docker-compose up -d 
3. docker ps
4. docker exec -it <container_id> //bin/bash
5. ./bin/spark-submit     --master spark://spark-master:7077     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1     /opt/spark-apps/consumer_stream.py