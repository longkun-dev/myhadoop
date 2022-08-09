package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:50 PM
 * @description mapper2 测试
 */
public class MaxAvgTempMapper2Test {

    @Test
    public void test1() {
        String line = "00_20220722";
        System.out.println(line.substring(0, 2));
        System.out.println(line.substring(7));
    }

    @Test
    public void map() throws IOException {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxAvgTempMapper2())
                .withInput(new LongWritable(1), new Text("00_20210721\t24"))
                .withOutput(new Text("00_0721"), new IntWritable(24))
                .runTest();
    }
}
