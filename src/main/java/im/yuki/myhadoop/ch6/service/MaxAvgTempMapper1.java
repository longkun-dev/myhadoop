package im.yuki.myhadoop.ch6.service;

import im.yuki.myhadoop.ch6.entity.TempRecord1;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:13 PM
 * @description 每年某一天最高气温的平均值
 * 数据格式 00_20220722_0900_4 表示 00 气象站在 2022年7月22日上午9时 温度为 4 摄氏度
 */
public class MaxAvgTempMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final TempRecord1 record = new TempRecord1();

    enum InvalidLineCount {
        INVALID_LINE_COUNT
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // 过滤掉分隔用的行
        if (line.length() < 17) {
            System.err.println("无效行，值为: " + line);
            context.setStatus("检测到无效的行，请查看日志");
            // 无效行计数器加 1
            context.getCounter(InvalidLineCount.INVALID_LINE_COUNT).increment(1);
            return;
        }
        record.parse(line);
        context.write(new Text(record.getStation() + "_" + record.getDate()),
                new IntWritable(record.getTemp()));
    }
}
