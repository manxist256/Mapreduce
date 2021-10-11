package wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Driver {

    public static class YReducer extends Reducer<javax.xml.soap.Text, IntWritable, javax.xml.soap.Text, IntWritable> {

        public void reduce(javax.xml.soap.Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            final IntWritable result = new IntWritable();

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }

    }

    public static class XMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            String[] words = line.toString().split(" ");
            for (String word: words) {
                Text text = new Text();
                text.set(word);
                context.write(text, one);
            }
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wc");

        job.setMapperClass(XMapper.class);
        job.setCombinerClass(YReducer.class);
        job.setReducerClass(YReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
