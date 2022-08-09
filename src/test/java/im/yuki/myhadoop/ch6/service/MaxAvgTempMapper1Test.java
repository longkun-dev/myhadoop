package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:28 PM
 * @description mapper 测试
 */
public class MaxAvgTempMapper1Test {

    @Test
    public void map() throws IOException {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new MaxAvgTempMapper1())
                .withInput(new LongWritable(1), new Text("00_20220722_0900_4"))
                .withOutput(new Text("00_20220722"), new IntWritable(4))
                .runTest();
    }
}
