package Events.parse;

/**
 * @description: Dreams need to be realized
 * @author: Jiang
 * @project:Data_String
 * @packate:parse
 * @date: 2020/3/8 16:14
 *
 *
 * whole situation -> parser
 */
public abstract class UserParser<T> implements Parsable<T> {
    /**
     * @Param: [fields]
     * @return: java.lang.Boolean
     * @Author: 江秋平
     * @Date: 2020/3/8
     *
     * 解耦
     */

    @Override
    public Boolean isHeader(String[] fields) {
        //check
        return (isValid(fields) && fields[0].equals("user_id") && fields[1].equals("locale") && fields[2].equals("birthyear")
                && fields[3].equals("gender") && fields[4].equals("joinedAt") && fields[5].equals("location") && fields[6].equals("timezone"));
    }

    @Override
    public Boolean isValid(String[] fields) {
        return (fields.length > 6 && !isEmpty( fields, new int[]{0} )  ); //  ?
    }
}
