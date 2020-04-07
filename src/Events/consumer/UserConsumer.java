package Events.consumer;

import Events.Writer.HbaseWriter;
import Events.Writer.Persistable;
import Events.parse.Hbase_UsersParser;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:consumer
 * @date: 2020/3/8 17:13
 */
// jiang.consumer.UserConsumer
public class UserConsumer extends MyConsumer {
    @Override
    protected Boolean getKafkaAutoCommit() {
        return false;
    }

    @Override
    protected String getKafkaTopic() {
        return "users";
    }

    @Override
    protected int getMaxPolledRecords() {
        return 2000;
    }

    @Override
    protected int getMaxPolledIntervalMillisecond() {
        return 6000;
    }

    @Override
    protected String getKafkaConsumerGrp() {
        return "groupUsers";
    }

    @Override
    protected Persistable[] getWriters() {
        return new Persistable[]{
          new HbaseWriter("events_db:users",new Hbase_UsersParser())
        };
    }
}
