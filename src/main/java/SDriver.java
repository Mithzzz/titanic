

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
public class SDriver {

    public static void main(String[] args) throws Exception {
        SDriver riDriver = new SDriver();
        Path inp = new Path(args[0]);
        Path outp = new Path(args[1]);
        riDriver.run(inp, outp);

    }

    public void run(Path inp, Path outp) throws Exception {


        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(SDriver.class);
        job.setMapperClass(SMapper.class);
        job.setReducerClass(SReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job,inp);

        FileOutputFormat.setOutputPath(job,outp);

        if(job.waitForCompletion(true)){
            System.out.println("Job Completed Successfully");
        }
    }

}
