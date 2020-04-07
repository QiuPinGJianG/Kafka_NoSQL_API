package Events.consumer;

import Events.Writer.Persistable;
import Events.config.MyConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;


import java.util.Arrays;
import java.util.Properties;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 *
 * multiple subClass, so like-> (UsersConsumer TrainConsumer.. so it is a superClass)
 * There are four core method:
 *     getWriters -> is a abstract
 *     execute -> extends IngestionExecutor in execute
 *     consume -> core method
 *     initialize -> initialization
 *
 */
public abstract class MyConsumer implements  IngestionExecutor{

    private String kafkaBrokerUrl = null;
    private Persistable[] writers;


    /**
     * @description: Dreams need to be realized
     * @author: Jiang
     *
     * method -> abstract
     *   give subClass extends
     *   1, return true and false
     *   2, return String is a topic name
     *   3, return int Max polled records num
     *   4, return int Max polled interval millisecond is a time
     *   5. return String ConsumerGroup
     */
    // Kafka offset Auto commit
    protected abstract Boolean getKafkaAutoCommit();
    // Kafka topic -> abstract
    protected abstract String getKafkaTopic();
    // Kafka MaxPolled -> records num
    protected abstract int getMaxPolledRecords();
    // Kafka MaxPolled -> Millisecond time
    protected abstract int getMaxPolledIntervalMillisecond();
    // Kafka jiang.consumer Group
    protected abstract String getKafkaConsumerGrp();


    // 构造
    public MyConsumer(){
        this.writers = this.getWriters();
    }

    protected abstract Persistable[] getWriters();
    /**
     * @Param: [props]
     * @return: void
     * @Author: 江秋平
     * @Date: 2020/3/8
     *  初始化
     */
    public void initialize(Properties props)  { // 此处Properties 是一个java本身类
        // load 放入配置文件
        this.kafkaBrokerUrl = props.getProperty(MyConfig.kafkaBrokerUrl);
        // initialize
        for ( Persistable i : this.writers) {
            //call
            // 在UserConsumer中 have a is method 是 getWriter(且重写本类 abstract getWriter)
            // 及上 UserConsumer中的实现方法 可以有数组 （hbase and mongoDB and MySql）
            // 及上 本类MyConsumer 为 解耦 的super class
            i.initialize(props);
            // 此 i 为 new HbaseWriter
        }
    }

    // kafka is a jiang.consumer 消费中
    protected void consume() throws Exception {
        // if kafkaUrl  不正确 就throws
        if(this.kafkaBrokerUrl == null || this.kafkaBrokerUrl.isEmpty()){
            throw new Exception("The Kafka broker url is not initialized.");
        }
        //print
        System.out.println("The Kafka BrokerUrl --> " + this.kafkaBrokerUrl);
        // -----------------------------------------------
        // prepare（准备） properties for the jiang.consumer
        Properties props = new Properties();
        //the kafka servers
        props.put("bootstrap.servers", this.kafkaBrokerUrl);
        //the jiang.consumer group
        props.put("group.id", this.getKafkaConsumerGrp());
        //flag for whether or not to commit the offset automatically
        props.put("enable.auto.commit", this.getKafkaAutoCommit() ? "true" : "false");
        //		//default option for resetting the offset
        props.put("auto.offset.reset", "earliest");  //another option: latest
        //the max # of records for each poll
        props.put("max.poll.records", Integer.toString(this.getMaxPolledRecords()));
        //the amount of time for the processing thread to process the current batch of messages
        props.put("max.poll.interval.ms", Integer.toString(this.getMaxPolledIntervalMillisecond()));
        //key & value serializer
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // -----------------------------------------------
        // create jiang.consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        // subscribe 订阅
        kafkaConsumer.subscribe(Arrays.asList(new String[] {getKafkaTopic()})); // 可能有多个topic ，所以用数组
        //print message
        System.out.println("Consumer subscribed to topic -> " + getKafkaTopic());

        try{
            // Processed 被处理过
            long msgPolled = 0L , msgProcessed = 0L;
            // loop for reading
            while (true) {
                // poll records
                ConsumerRecords<String, String> records = kafkaConsumer.poll(3000);
                // number of records
                // 读多少了
                int recordsCount = (records != null) ? records.count() : 0;
                // 读累了 休息多久 哈哈
                if ( recordsCount <= 0 ) {
                    // sleep for 3 seconds
                    Thread.sleep(3000);
                    // Go next
                    continue;
                }

                // 一共读多少 记录了
                msgPolled += recordsCount;

                // *****************************************
                // Hbase pull hbase拉取records
                for (Persistable writer : writers){
                    //writer
                    msgProcessed += writer.write( records );
                }
                // 提交方式的判断
                if ( !this.getKafkaAutoCommit() ){
                    //commit
                    kafkaConsumer.commitSync();
                }


                // *****************************************
                //print
                System.out.print(String.format("**** %d messages polled, %d processed! -----", msgPolled, msgProcessed));
            }

        }finally{
            kafkaConsumer.close();
        }
    }

    //main
    @Override
    public void execute(String[] args) throws Exception {

        if (args.length < 1){
            System.out.println(String.format("Usage: %s <settings-file>", this.getClass().getName()));
            System.out.println("<settings-file>: the configuration settings");
        }else{
            // 1,initialize
            this.initialize(MyConfig.loadSettings(args[0]));
            this.consume();
        }
    }
}
