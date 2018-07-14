

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created by mithu on 14/7/18.
 */
public class TDriver {

    public static void main(String[] args) throws Exception {
        TDriver riDriver = new TDriver();
        Path inp = new Path(args[0]);
        Path outp = new Path(args[1]);
        riDriver.run(inp, outp);

    }

    public void run(Path inp, Path outp) throws Exception {


        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(TDriver.class);
        job.setMapperClass(TMapper.class);
        job.setReducerClass(TReducer.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);



        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);



        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job,inp);

        FileOutputFormat.setOutputPath(job,outp);

        if(job.waitForCompletion(true)){
            System.out.println("Job Completed Successfully");
        }
    }

}
