package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:45 PM
 * @description 从某个气象站一天最高气温，不含年份
 * <p>
 * 输入数据格式：
 * 00_20220722 9
 * 00_20210722 6
 * <p>
 * 转换后数据：
 * 00_0722 9
 * 00_0722 6
 */
public class MaxAvgTempMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] pair = line.split("\\s");
        String key1 = pair[0];
        String newKey = key1.substring(0, 2) + "_" + key1.substring(7);
        context.write(new Text(newKey), new IntWritable(Integer.parseInt(pair[1])));
    }
}
