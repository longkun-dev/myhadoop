package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/30 11:15 PM
 * @description 合并小文件 mapper
 */
public class MergeMapper extends Mapper<NullWritable, Text, IntWritable, Text> {

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(1), value);
    }
}
