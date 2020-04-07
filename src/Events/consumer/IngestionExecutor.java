package Events.consumer;

 /*
 做为 MyConsumer 的接口
  */

public interface IngestionExecutor {
    //
     void execute (String[] args) throws Exception;
}
