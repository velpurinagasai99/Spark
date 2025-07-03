#we need java and zookeper up and running. To run all the kafka commands first navigate to kafka folder, there we have executable files
#to create a server we execute -server-start.sh with the properties file which is located in config folder..below command
./kafka-server-start.sh ../config/server.properties

#to create a topic, server(here localhost:9092) should be up and running with following command....below is list down the topics
./kafka-topics.sh --create --topic myfirsttopic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
./kafka-topics.sh --list --bootstrap-server localhost:9092

#to create a producer, we can also give multiple brokers list for back-up...data is stored in cd /tmp file
./kafka-console-produce.sh --broker-list localhost:9092 --topic myfirsttopic

#to create a consumer
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic myfirsttopic --from-beginning

#??? In Week17-Session5: While creating topic on 3 node cluster with 3 partitions, we are only specifying localhost:9092 we are not specifying localhost:9093, localhost:9094, still how it is considering 3 brokers ?
#I think it is cluster level management not broker-leve, all these nodes comes under one cluster....For high performance with horizontal scalability, Kafka distributes topic log partitions on different nodes in a cluster
#For failover management, Kafka replicates partitions to many nodes.Kafka Brokers contain topic log partitions. While Connecting to one broker bootstraps a client to the entire Kafka cluster. Hence you can see logs in all 3 brokers

#creating multi nodes and storing topics in them..as local, first create multiple nodes by changing the port numbers, brokerIds and logs directory location(server.properties file) and start running server the folder
./kafka-server-start.sh ../config/server1.properties
./kafka-server-start.sh ../config/server2.properties

./kafka-topics.sh --create --topic multinodestopic --bootstrap-server localhost:9092,localhost:9093 --partitions 3  --replication-factor 2
./kafka-topics.sh --list --bootstrap-server localhost:9092,localhost:9093
./kafka-console-consumer.sh --bootstrap-server localhost:9093,localhost:9094,localhost:9092 --topic multinodestopic --from-beginning

#for creating multiple consumers...run same command on different terminals to create different consumers
./kafka-topics.sh --create --topic customers --bootstrap-server localhost:9092,localhost:9093 --partitions 3  --replication-factor 1
./kafka-console-consumer.sh --bootstrap-server  localhost:9092 --topic customers --from-beginning --group customergroups
./kafka-console-producer.sh --broker-list localhost:9092 --topic customers < ../../../Desktop/customersdata.csv

#to check which is controlling the brokers
./zookeeper-shell.sh localhost:2181
