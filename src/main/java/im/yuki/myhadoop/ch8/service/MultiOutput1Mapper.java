package im.yuki.myhadoop.ch8.service;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/2 10:56 PM
 * @description 使用 MultiOutput 类按照气象站 id 输出多个文件
 */
public class MultiOutput1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String stationId = line.substring(0, 2);
        context.write(new Text(stationId), value);
    }
}
