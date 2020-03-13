#本地
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic dau --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic install --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic counter --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic economy --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic gameinfo --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic milestone --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic payment --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic adtracking --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper zook01:2181 --topic custombinarybodymetric --replication-factor 2 --partitions 5

#集群比较大
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic dau --replication-factor 2 --partitions 20
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic install --replication-factor 2 --partitions 10
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic counter --replication-factor 2 --partitions 30
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic economy --replication-factor 2 --partitions 20
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic gameinfo --replication-factor 2 --partitions 20
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic milestone --replication-factor 2 --partitions 20
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic payment --replication-factor 2 --partitions 10
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic adtracking --replication-factor 2 --partitions 30
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic custombinarybodymetric --replication-factor 2 --partitions 10

#集群比较小
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic dau --replication-factor 2 --partitions 2
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic install --replication-factor 2 --partitions 2
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic counter --replication-factor 2 --partitions 5
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic economy --replication-factor 2 --partitions 3
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic gameinfo --replication-factor 2 --partitions 2
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic milestone --replication-factor 2 --partitions 3
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic payment --replication-factor 2 --partitions 2
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic adtracking --replication-factor 2 --partitions 2
bin/kafka-topics.sh --create --zookeeper ka01:2181,ka02:2181 --topic custombinarybodymetric --replication-factor 2 --partitions 2