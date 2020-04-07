package Events.transaction;

import Events.config.MyConfig;
import Events.consumer.IngestionExecutor;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.internals.Topic;

import java.util.*;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:Events.transaction
 * @date: 2020/3/11 16:35
 *
 *  -> consumer -> produce (transaction and commit offset)
 */
public abstract class MyPipeline implements IngestionExecutor {
    private String zookeeperUrl = null;
    private String kafkaBrokerUrl = null;

    protected abstract String getConsumerAppId();
    protected abstract String getConsumerTopic();
    protected abstract String getProducerAppId();
    protected abstract String getProducerTopic();
    public MyPipeline(){}


    @Override
    public void execute(String[] args) throws Exception {

        if (args.length < 1){
            System.out.println(String.format("Usage: %s <settings-file>", this.getClass().getName()));
            System.out.println("<settings-file>: the configuration settings");
        }else{
            try {
                // 1,initialize
                this.initialize(MyConfig.loadSettings(args[0]));
                this.run();
            }
            catch (Exception e){
                e.printStackTrace();
        }
    }

    }

    public abstract List<ProducerRecord> transformTo(ConsumerRecord<String, String> record , String producerTopic);

    private void run() throws Exception {
        KafkaConsumer<String, String> consumer = createConsumer();
        consumer.subscribe(Arrays.asList( new String[] { getConsumerTopic()}) );
        try{
            KafkaProducer<String, String> producer = createProducer();
            producer.initTransactions();
            while(true){
                // 收数据
                ConsumerRecords<String, String> records = consumer.poll(3000);
                int recordCount = (records != null ) ? records.count() : 0;

                if (recordCount <= 0){
                    Thread.sleep(3000);
                    continue;
                }
                producer.beginTransaction();

                // -------------------------------------------
                try{
                    for (ConsumerRecord<String, String> record : records){
                        for (ProducerRecord<String, String> producerRecord: transformTo(record,this.getProducerTopic()) ){
                            producer.send(producerRecord);
                        }
                    }
                    Map<TopicPartition, OffsetAndMetadata> offsets = calculateConsumerOffset(records);
                    // 这个consumer id 是拿数据的consumer id
                    producer.sendOffsetsToTransaction(offsets, this.getConsumerAppId());
                    producer.commitTransaction();
                }catch (KafkaException e){
                        producer.abortTransaction();
                        throw e;
                }
                // 从一个地方拿数据 然后经过consumer ，
                // 然后我们的producer 开始 transaction
                // 和提交offsets
                // 在发送给另外一个topic

                // -------------------------------------------

            }

        }
        finally {
            consumer.close();
        }


    }
    // calculate commit offset
    private Map<TopicPartition, OffsetAndMetadata> calculateConsumerOffset(ConsumerRecords<String, String> records) {
        HashMap<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        // for 循环的是records 里面的 partition
        for (TopicPartition partition : records.partitions()){
            // 从当前partition 拿过来的所有记录
            List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
            // 记录大小-1 ， 找最后一个record的offset
            long offset = partitionRecords.get(partitionRecords.size() - 1).offset();
            // 生成新offset
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        return offsetsToCommit;
    }

    private KafkaProducer<String, String> createProducer(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,this.kafkaBrokerUrl);
        // transaction id
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,this.getProducerAppId());
        // 重试
        props.put(ProducerConfig.RETRIES_CONFIG,Integer.MAX_VALUE);
        // 批 大小
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);
        // 幂等性
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        // ack -1
        props.put(ProducerConfig.ACKS_CONFIG,"all");
        // key serializer
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // value serializer
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<String, String>(props);

    }

    private KafkaConsumer<String, String> createConsumer() throws Exception{
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaBrokerUrl);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,this.getConsumerAppId());
        // flag fo enable auto commit
        // 设置自动提交偏移量
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        // the isolation level
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed");
        // offset 重置机制
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // 最大投递消息的大小
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,500);
        // 最大投递消息的间隔
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,9000);
        //key serializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //value serializer
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<String, String>(props);
    }

    private void initialize(Properties props) {
        this.zookeeperUrl = props.getProperty(MyConfig.zooKeeperUrl);
        this.kafkaBrokerUrl = props.getProperty(MyConfig.kafkaBrokerUrl);
    }
}
