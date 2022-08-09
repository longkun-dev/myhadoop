package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 12:19 AM
 * @description 统计单词个数的 mapper
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line == null) {
            return;
        }

        // 分割每一行字符串
        String[] strings = line.split("!|\\s|\\.");
        if (strings.length == 0) {
            return;
        }
        for (String str : strings) {
            context.write(new Text(str), new IntWritable(1));
        }
    }
}
