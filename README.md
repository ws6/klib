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


## when use amqp for long running functions
#
#the latch to enable or disable amqp; if it is not a "true" (case sensitive), it will not forward message to RMQ
use_amqp=true
#the connection string for amqp
amqp_connection_string=amqp://$username:$password@host:$port/
#queue name used in rmq. create if not exist
amqp_queue_name=the_queue_name_in_rmq
### DO NOT use this approach for high though put non-persistency needed scenarios. it will congest RMQ