import org.apache.hadoop.mapred.lib.db.DBWritable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Taras S. on 05.05.15.
 */
public class DBOutputWritable implements DBWritable {

    private String word;

    private int count;

    public DBOutputWritable(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public void readFields(ResultSet resultSet) throws SQLException {
        word = resultSet.getString(1);
        count = resultSet.getInt(2);
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, word);
        preparedStatement.setInt(2, count);
    }

}
