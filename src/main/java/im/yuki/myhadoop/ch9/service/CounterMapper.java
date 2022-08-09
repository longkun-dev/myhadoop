package im.yuki.myhadoop.ch9.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/4 11:21 PM
 * @description 自定义计数器
 */
public class CounterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    enum Length {
        /**
         * 行长度超过 5 个字符的计数器
         */
        LENGTH_THAN_5_CNT,

        /**
         * 行长度超过 8 个字符的计数器
         */
        LENGTH_THAN_8_CNT,

        /**
         * 其他的统计计数器
         */
        OTERH_CNT
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        int length = line.length();
        if (length > 8) {
            context.getCounter(Length.LENGTH_THAN_8_CNT).increment(1);
        } else if (length > 5) {
            context.getCounter(Length.LENGTH_THAN_5_CNT).increment(1);
        } else {
            context.getCounter(Length.OTERH_CNT).increment(1);
        }

        // 动态计数器
        context.getCounter("DynamicCounter", "allLineCount").increment(1);

        context.write(new IntWritable(length), value);
    }
}
