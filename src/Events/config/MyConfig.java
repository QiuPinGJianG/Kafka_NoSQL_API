package Events.config;

import java.io.*;
import java.util.Properties;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:config
 * @date: 2020/3/8 14:52
 */
public class MyConfig {

    //zoo-keeper url
    public final static String zooKeeperUrl = "zookeeperUrl";
    //broker url
    public final static String kafkaBrokerUrl = "brokerUrl";
    //state directory
    public final static String stateDir = "stateDir";

    //core-site
    public final static String coreSite = "coreSite";
    //hdfs-site
    public final static String hdfsSite = "hdfsSite";
    //hbase-site
    public final static String hbaseSite = "hbaseSite";

    //the host of the mongo server
    public final static String mongoHost = "mongoHost";
    //the port of the mongo server for client connection
    public final static String mongoPort = "mongoPort";
    //the target mongo database
    public final static String mongoDatabase = "mongoDatabase";
    //the user used for connecting to mongo
    public final static String mongoUser = "mongoUser";
    //password
    public final static String mongoPwd = "mongoPwd";

    //db jdbc-url
    public final static String dbJdbcUrl = "dbJdbcUrl";
    //user for connecting to db
    public final static String dbUser = "dbUser";
    //user's password
    public final static String dbPassword = "dbPassword";

    public static Properties loadSettings(String settingsFile) throws IOException {
        // initialize Properties
        Properties props = new Properties();
        // oper file
        FileInputStream fileInputStream = new FileInputStream(settingsFile);
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
            // reader file
            BufferedReader br = new BufferedReader(inputStreamReader);
            try{
                // read first line
                String firstLine = br.readLine();

                // loop for read
                while(firstLine != null){
                    String[] kv = firstLine.split("=", -1);

                    if (kv != null && kv.length ==2 ){
                        props.put(kv[0], kv[1]);
                    }
                    // read next line
                    firstLine = br.readLine();
                }
            }finally{
                br.close();
            }

        }finally {
            fileInputStream.close();
        }

        return props;
    }


}
