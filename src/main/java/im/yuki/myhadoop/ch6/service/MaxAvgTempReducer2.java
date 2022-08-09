package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:53 PM
 * @description reducer2
 * <p>
 * 输入数据格式：
 * 00_0722 10
 * 00_0722 8
 * <p>
 * 输出数据格式：
 * 00_0722 9
 */
public class MaxAvgTempReducer2 extends Reducer<Text, IntWritable, Text, FloatWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        float averageTemp;
        int sum = 0;
        int count = 0;
        for (IntWritable value : values) {
            sum += value.get();
            count++;
        }
        averageTemp = BigDecimal.valueOf(sum).divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP)
                .setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();
        context.write(key, new FloatWritable(averageTemp));
    }
}
