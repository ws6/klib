### klib
confluent kafka client producer and consumer wrapper

 

## configuration keys map
consumer_group_id  = the consumer group. we default to rely on this without self-managing the offset/partitions

bootstrap.servers = semicolon delimited list of brokers

batch_size  = batch size for produce messages, default 1

sasl.username  = username
sasl.password = password


num_partitions_${topic} = topic specific number of partions when create topic,default 1 
replication_factor = broker specific configuration,default 3. if not match with server, create topic will fail 
disable_dlq_${topic} = if explicity disabled dlq of a topic. "true" for enable . default enabled

