package Events.Driver;

import Events.consumer.IngestionExecutor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import java.util.Arrays;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:Driver
 * @date: 2020/3/8 17:20
 */

@SpringBootApplication
public class Driver implements CommandLineRunner {

    public static void main(String[] args) {

        System.out.println(String.format("_____________________________11111_______________________________________"));
        //run
        SpringApplication.run(Driver.class, args);

    }
    @Override
    public void run(String... args) throws Exception {
        if (args == null || args.length < 1){
            // error out
            throw new Exception("Please specify the executor class.");
        }

        // create instance
        // 通过放射 将command 的输入参数的类 进行反射
        // reflect
        Object o = Class.forName(args[0]).newInstance();

        if ( o instanceof IngestionExecutor ){
            ( (IngestionExecutor)o ).execute(Arrays.copyOfRange(args, 1, args.length));
        }else{
            // error out
            throw new Exception("The specified Executor is not an IngestionExecutor.");
        }


    }
}
