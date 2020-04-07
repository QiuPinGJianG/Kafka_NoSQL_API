package Events.parse;


import org.apache.commons.lang.ArrayUtils;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:parse
 * @date: 2020/3/8 14:22
 */
public interface Parsable<T>{

    /**
     * @Param: [fields]
     * @return: java.lang.Boolean
     * @Author: 江秋平
     * @Date: 2020/3/8
     * abstract check Header
     */
    Boolean isHeader(String[] fields);

    /***
     * @Param: [fields]
     * @return: java.lang.Boolean
     * @Author: 江秋平
     * @Date: 2020/3/8
     * abstract check Valid
     */
    Boolean isValid(String[] fields);

    /***
     * @Param: [fields, indexs]
     * @return: java.lang.Boolean
     * @Author: 江秋平
     * @Date: 2020/3/8
     */

    default Boolean isEmpty(String[] fields, int[] indexs){
        Boolean empty = false;
        // 1,if -> 先check 一条record 是不是null  and   >0
        // 2,for -> check indexs 数组是不是 null ，遍历
        // 3,if -> check 每个元素（record 中的 一个个成员，比喻users -> users_id , Birthyear..）是不是为null
        if (fields != null && fields.length > 0){
            for (int i = 0; i < fields.length; i++) {
                // contains -> check 该数据在该数组中是否存在，返回一个boolean值
                if (indexs != null && ArrayUtils.contains(indexs,i)){
                    // 1，empty = empty || (fields[i] == null || fields[i].trim().length() <= 0)
                    // 2，fields[i].trim() ->trim()方法实际上trim掉了字符串两端Unicode编码小于等于32（\u0020）的所有字符。
                    empty |= (fields[i] == null || fields[i].trim().length() <= 0);
                }
            }
        }
        return empty;
    }

    /***
     * @Param: [fields]
     * @return: T
     * @Author: 江秋平
     * @Date: 2020/3/8
     *
     * jiang.parse the record in -> a record
     */
    T parse (String[] fields);


}
