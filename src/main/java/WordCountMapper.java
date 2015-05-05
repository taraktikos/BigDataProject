import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Taras S. on 05.05.15.
 */
public class WordCountMapper  extends Mapper<LongWritable, DBInputWritable, Text, IntWritable> {

    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, DBInputWritable value, Context context) throws IOException, InterruptedException {
        Set<String> keys = new HashSet<String>(Arrays.asList(value.getText().split(" ")));

        for (String k : keys) {
            context.write(new Text(k), one);
        }
    }

}
