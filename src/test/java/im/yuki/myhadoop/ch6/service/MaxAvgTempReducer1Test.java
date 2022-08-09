package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:42 PM
 * @description reducer1 测试
 */
public class MaxAvgTempReducer1Test {

    @Test
    public void reduce() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new MaxAvgTempReducer1())
                .withInput(new Text("00_20220722"), new ArrayList<IntWritable>() {{
                    add(new IntWritable(10));
                    add(new IntWritable(12));
                    add(new IntWritable(14));
                }})
                .withOutput(new Text("00_20220722"), new IntWritable(14))
                .runTest();
    }
}
