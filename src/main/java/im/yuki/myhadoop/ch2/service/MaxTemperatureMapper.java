package im.yuki.myhadoop.ch2.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/18 9:32 PM
 * @description 寻找每年的最高气温
 * 气温文件地址: files/ch2/max_temperature/temperature.txt
 */
public class MaxTemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // 方便起见，自定义数据格式：
    // 20200101_10
    // 20200102_5
    // 20200103_
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(0, 4);
        String temperatureStr = line.substring(9);
        if (StringUtils.isBlank(temperatureStr)) {
            System.out.println("记录中气温值不存在: " + value);
        } else {
            int temperature = Integer.parseInt(temperatureStr);
            context.write(new Text(year), new IntWritable(temperature));
        }
    }
}
