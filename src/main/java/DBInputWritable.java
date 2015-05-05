import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by Taras S. on 05.05.15.
 */
public class DBInputWritable implements DBWritable {

    private int id;
    private String text;

    public void readFields(ResultSet resultSet) throws SQLException {
        id = resultSet.getInt(1);
        text = resultSet.getString(3);
    }

    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1, id);
        preparedStatement.setString(3, text);
    }

    public int getId() {
        return id;
    }

    public String getText() {
        return text;
    }
}
