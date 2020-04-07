package Events.transaction;

import Events.config.MyConfig;
import Events.consumer.IngestionExecutor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Properties;

// Events.transaction.MyTransactionProducer
/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:Events.transaction
 * @date: 2020/3/10 19:25
 */
public class MyTransactionProducer implements IngestionExecutor {

    @Override
    public void execute(String[] args) throws Exception {
        if (args.length != 3){
            System.out.println("class name + topic name + settingsFile name");
        }else {
            String brokerURL = MyConfig.loadSettings(args[0]).getProperty(MyConfig.kafkaBrokerUrl);
            System.out.println("this Kafka URL is " + brokerURL);
            String topicName = args[1].toString();
            System.out.println("this Topic name  is " + topicName);
            String fileName = args[2].toString();
            System.out.println("this File name is " + fileName);
            // create an instance for properties to access the producer configs
            // 为属性创建一个实例来访问生产者配置
            Properties props = new Properties();
            // assign localhost id
            // 分配指定id
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerURL);
            // open 幂等性
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
            // transaction id
            props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my_transaction_id");
            // 重试的次数
            props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
            //  ack的Level
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            //  指定buffer大小(也就是指定batch的大小)
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
            //key serializer
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            //value serializer
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            // create KafkaProducer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
            producer.initTransactions();
            try{
                File file = new File(fileName);
                FileReader fileReader = new FileReader(file);
                BufferedReader br = new BufferedReader(fileReader);

                long pos =0 , count = 0;

                try {
                    producer.beginTransaction();
                    // read one line
                    String text = br.readLine();
                    while (text != null) {
                        // adjust position
                        pos += text.length() + 2;
                        // send
                        producer.send(new ProducerRecord<String, String>(topicName, Long.toString(pos), text));
                        count++;

                        text = br.readLine();
                    }
                    producer.flush();
                    producer.commitTransaction();
                }
                catch (KafkaException e){
                    producer.abortTransaction();
                    throw e;
                }
                finally {
                    br.close();
                }
            }catch (java.lang.Exception e){
                e.printStackTrace();
            }finally {
                producer.close();

            }


        }
    }
}
