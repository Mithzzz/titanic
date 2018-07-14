import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by mithu on 14/7/18.
 */
public class SMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text people = new Text();
    private final static IntWritable one = new IntWritable(1);
    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
        String line = value.toString();
        String str[]=line.split(",");
        if(str.length>6){
            String survived=str[1]+" "+str[4]+" "+str[5];
            people.set(survived);
            context.write(people, one);

        }
    }

}
