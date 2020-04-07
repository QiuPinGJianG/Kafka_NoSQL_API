package Events.parse;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:parse
 * @date: 2020/3/8 16:16
 *
 * 一个具体到 Hbase 的那个table 的 parser class
 */
public class Hbase_UsersParser extends UserParser {

    //jiang.parse the record
    public Put parse(String[] fields) {
        //user_id, locale, birth_year, gender, joined_at, location, time_zone
        //user id
        Put p = new Put(Bytes.toBytes(fields[0]));

        //profile: birth_year
        p = p.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("birth_year"), Bytes.toBytes(fields[2]));
        //profile: gender
        p = p.addColumn(Bytes.toBytes("profile"), Bytes.toBytes("gender"), Bytes.toBytes(fields[3]));

        //region: locale
        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("locale"), Bytes.toBytes(fields[1]));
        //region: location
        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("location"), Bytes.toBytes(fields[5]));
        //region: time-zone
        p = p.addColumn(Bytes.toBytes("region"), Bytes.toBytes("time_zone"), Bytes.toBytes(fields[6]));

        //registration: joined_at
        p = p.addColumn(Bytes.toBytes("registration"), Bytes.toBytes("joined_at"), Bytes.toBytes(fields[4]));

        //result
        return p;
    }
}
