package Events.Writer;

import Events.config.MyConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import Events.parse.Parsable;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:Writer
 * @date: 2020/3/8 14:18
 */
public class HbaseWriter implements Persistable {

    //core-site xml file
    private String coreSite = null;
    //hdfs-site xml file
    private String hdfsSite = null;
    //hbase-site xml file
    private String hbaseSite = null;
    //the hbase table
    private String hbTable = null;
    //the parser
    private Parsable<Put> parser = null;

    //constructor
    public HbaseWriter(String hbTable, Parsable<Put> parser) {
        //set
        this.hbTable = hbTable;
        this.parser = parser;
    }

    //initialize to extract the hbase-site configuration
    public void initialize(Properties props) {
        //core-site
        this.coreSite = props.getProperty(MyConfig.coreSite);
        //hdfs-site
        this.hdfsSite = props.getProperty(MyConfig.hdfsSite);
        //hbase
        this.hbaseSite = props.getProperty(MyConfig.hbaseSite);
    }

    //write
    public int write( ConsumerRecords<String, String> records ) throws Exception {
        //the # of records puts
        int numPuts = 0;
        //check
        if ( this.hbaseSite == null || this.hbaseSite.isEmpty() ) {
            //error out
            throw new Exception("The hbase-site.xml is not initialized.");
        }

        //configuration
        Configuration cfg = HBaseConfiguration.create();
        //check
        if ( this.coreSite != null ) {
            //set resource
            cfg.addResource( new Path(this.coreSite) );
        }
        if ( this.hdfsSite != null ) {
            //set resource
            cfg.addResource( new Path(this.hdfsSite) );
        }
        //the hbase-site
        cfg.addResource( new Path(this.hbaseSite) );

        //establish a connection
        Connection conn = ConnectionFactory.createConnection( cfg );
        try {
            //HTable
            Table tbl = conn.getTable( TableName.valueOf(this.hbTable) );
            try {
                //collection
                List<Put> puts = new ArrayList<Put>();
                //flags
                long passHead = 0;
                //loop
                for ( ConsumerRecord<String, String> record : records ) {
                    try {
                        //jiang.parse event record
                        String[] elements = record.value().split(",", -1);
                        //check if the head has been passed
                        if ( passHead == 0 && this.parser.isHeader(elements) ) { // 开始的时候 是不是读到了 头信息
                            //flag
                            passHead = 1;
                            //skip
                            continue;
                        }

                        //jiang.parse
                        if ( this.parser.isValid(elements) ) {  //是不是有效
                            //add
                            puts.add( this.parser.parse(elements) );
                        }
                        else {
                            //print error
                            System.out.println(String.format("ErrorOccured: invalid message found when writing to HBase! - %s", record.value()));
                        }
                    }
                    catch (Exception e ) {
                        //print error
                        System.out.println("ErrorOccured: " + e.getMessage());
                    }
                }
                //check
                if ( puts.size() > 0 ) {
                    //save
                    tbl.put( puts );  // put 进 hbase
                }

                //set
                numPuts = puts.size();
            }
            finally {
                //close the table
                tbl.close();
            }
        }
        finally {
            //close the connection
            conn.close();
        }
        return numPuts;
    }
}
