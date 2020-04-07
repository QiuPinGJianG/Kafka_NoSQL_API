package Events.Writer;


import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.Properties;

/**
 * @description: Dreams need to be realized
 * @author: Qiu
 * @date: 2020/3/8 13:48
 * 持久
 */
public interface Persistable {
    /**
     * @Param: [props]
     * @return: void
     * @Author: 江秋平
     * @Date: 2020/3/8
     * give HbaseWriter extends initialize
     */
    void initialize(Properties props);
    /**
     * @Param: [records]
     * @return: int
     * @Author: 江秋平
     * @Date: 2020/3/8
     */
    int write (ConsumerRecords<String, String > records) throws Exception;


}
