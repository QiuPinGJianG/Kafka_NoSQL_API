package Events.transaction;

import org.apache.commons.lang.ArrayUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:Events.transaction
 * @date: 2020/3/11 17:57
 */

// Events.transaction.UserFriendsPipeline
public class UserFriendsPipeline extends MyPipeline {
    @Override
    protected String getConsumerAppId() {
        return "My_Pipeline_Consumer";
    }

    @Override
    protected String getConsumerTopic() {
        return "user_friends_raw";
    }

    @Override
    protected String getProducerAppId() {
        return "My_Pipeline_Producer";
    }

    @Override
    protected String getProducerTopic() {
        return "User_friends";
    }

    //check if a record is empty
    private Boolean isEmpty(String[] fields, int[] indexes) {
//        Boolean empty = false;
//        //check
//        if ( fields != null && fields.length > 0 ) {
//            //loop
//            for ( int i = 0; i < fields.length; i++ ) {
//                //check				//检查该数据在该数组中是否存在，返回一个boolean值。
//                if ( indexes != null && ArrayUtils.contains(indexes,  i) ) {
//                    //combine
//                    //  empty |=    -->      empty=empty|b
//                    empty |= (fields[i] == null || fields[i].trim().length() <= 0);
//                    //trim()方法实际上trim掉了字符串两端Unicode编码小于等于32（\u0020）的所有字符。
//                }
//            }
//        }
//        return empty;
        Boolean empty =false;
        if (fields != null && fields.length > 0) {
            for (int i = 0; i < fields.length; i++) {
                if (indexes != null && ArrayUtils.contains(indexes,i)) {
                    empty |= (fields[i] == null || fields[i].length() <= 0);
                }
            }
        }
        return empty;
    }

    //check if the record is a header
    public Boolean isHeader(String[] fields) {
        //check
        return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("friend_id"));
    }

    //check if a record is valid
    public Boolean isValid(String[] fields) {
        //check
        return (fields.length > 1 && !isEmpty(fields, new int[] { 0, 1 }));
    }

    @Override
    public List<ProducerRecord> transformTo(ConsumerRecord<String, String> record, String producerTopic) {
        List<ProducerRecord> producerRecords = new ArrayList<>();
        // 拿到从user_friends_raw 中的值，然后进行split
        String[] elements = record.value().split(",", -1);

        // check if the head has been passed
        // 检查头部是否通过
        if (!this.isHeader(elements) && this.isValid(elements)){
            // get 到 user firend
            String user = elements[0];
            String[] friends = elements[1].split(" ");
            if (friends != null && friends.length > 0){
                for (String friend : friends) {
                    String key = String.format("%s-%s", user, friend);
                    String value = String.format("%s,%s", user, friend);
                    producerRecords.add(new ProducerRecord<>(producerTopic,key,value));
                }
            }
        }
        return producerRecords;
    }
}
