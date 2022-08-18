package im.yuki.myhadoop.ch2.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/18 9:44 PM
 * @description 寻找每年的最高气温
 * 气温文件地址: files/ch2/max_temperature/temperature.txt
 */
public class MaxTemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // 输入数据格式
    // <2021: [8, 10, 20]>
    // <2022: [2, 8, 10]>
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int maxTemperature = Integer.MIN_VALUE;
        for (IntWritable value : values) {
            int tempVal = value.get();
            if (tempVal > maxTemperature) {
                maxTemperature = tempVal;
            }
        }
        context.write(key, new IntWritable(maxTemperature));
    }
}
