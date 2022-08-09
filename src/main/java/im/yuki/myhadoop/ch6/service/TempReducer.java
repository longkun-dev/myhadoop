package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/19 12:05 AM
 * @description reducer
 */
public class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemp = -999;
        for (IntWritable value : values) {
            maxTemp = Math.max(maxTemp, value.get());
        }
        context.write(key, new IntWritable(maxTemp));
    }
}
