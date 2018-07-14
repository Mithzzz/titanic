


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by mithu on 14/7/18.
 */
public class TReducer extends Reducer<Text,IntWritable, Text, IntWritable> {


    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int l=0;
        for (IntWritable val : values) {
            l+=1;
            sum += val.get();
        }
        sum=sum/l;
        context.write(key, new IntWritable(sum));
    }
}
