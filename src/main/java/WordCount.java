import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by Taras S. on 05.05.15.
 */
public class WordCount extends Configured  implements Tool {

    private static final String URL = "jdbc:mysql://localhost:3306/spring-blog";
    private static final String USERNAME = "root";
    private static final String PASSWORD = "root";

    public int run(String[] strings) throws Exception {
        tuncateOutputTable();

        Configuration conf = getConf();
        DBConfiguration.configureDB(conf, "com.mysql.jdbc.Driver", URL, USERNAME, PASSWORD);

        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJobName("Word post count");

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(DBOutputWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(DBInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);

        DBInputFormat.setInput(job, DBInputWritable.class, "posts", null, null, new String[]{"id", "created_at", "text"});
        DBOutputFormat.setOutput(job, "output", new String[] { "word", "count" });

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void tuncateOutputTable() throws SQLException {
        Connection connection = DriverManager.getConnection(URL, USERNAME, PASSWORD);
        PreparedStatement statement  = connection.prepareStatement("TRUNCATE `output`");
        statement.execute();
        statement.close();
        connection.close();
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount(), args));
    }
}
