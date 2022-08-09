package im.yuki.myhadoop.ch6.service;

import im.yuki.myhadoop.ch6.entity.TempRecord;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/18 11:31 PM
 * @description Mapper
 */
public class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final TempRecord record = new TempRecord();

    enum Temperature {
        OVER_100
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 获取分片相关的信息
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        System.out.println("inputSplitSize: " + fileSplit.getLength());
        System.out.println("path: " + fileSplit.getPath());
        System.out.println("start: " + fileSplit.getStart());

        String line = value.toString();
        record.parse(line);
        if (record.getTemp() > 8) {
            System.err.println("温度超过 8 度: " + record.getTemp());
            context.setStatus("检测到可能错误的输入值，请查看日志");
            context.getCounter(Temperature.OVER_100).increment(1);
        }
        context.write(new Text(record.getYear()), new IntWritable(record.getTemp()));
    }
}
