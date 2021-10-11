package wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class XMapper extends Mapper<Object, Text, Text, IntWritable> {
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
