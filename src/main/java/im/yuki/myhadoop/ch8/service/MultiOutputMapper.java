package im.yuki.myhadoop.ch8.service;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/2 10:11 PM
 * @description 从多个文件读取气象站数据，并解析
 */
public class MultiOutputMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 每行数据前两位为气象站 id
        String stationId = line.substring(0, 2);
        context.write(new Text(stationId), value);
    }
}
