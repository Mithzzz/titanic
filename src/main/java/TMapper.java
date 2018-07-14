import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by mithu on 14/7/18.
 */
public class TMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text gender = new Text();
    private IntWritable age = new IntWritable();
    public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
        String line = value.toString();
        String str[]=line.split(",");
        if(str.length>6){
            gender.set(str[4]);
            if((str[1].equals("0")) ){
                if(str[5].matches("\\d+")){
                    int i=Integer.parseInt(str[5]);
                    age.set(i);

                }
            }
        }
        context.write(gender, age);

    }

}
