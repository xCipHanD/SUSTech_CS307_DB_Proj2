package tmp;

import edu.sustech.cs307.exception.DBException;
import edu.sustech.cs307.exception.ExceptionTypes;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.parser.JSqlParser;
import net.sf.jsqlparser.statement.Statement;

import java.io.StringReader;

public class jsqlparser {
    public static void main(String[] args) {
        String sqlStr = "select * from A left join B on A.id=B.id and A.age = (select age from C)";
        JSqlParser parser = new CCJSqlParserManager();
        Statement stmt = null;
        try {
            stmt = parser.parse(new StringReader(sqlStr));
        } catch (JSQLParserException e) {

        }
    }
}
