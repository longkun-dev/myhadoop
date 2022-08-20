package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 12:27 AM
 * @description 测试字符串分割
 */
public class WordCountMapperTest {

    @Test
    public void test1() {
        String str = "hello world! nihao ba.a";
        String[] split = str.split("!|\\s|\\.");
        System.out.println(Arrays.toString(split));
    }

    @Test
    public void testMapper() throws IOException {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(1), new Text("hello world!"))
                .withOutput(new Text("hello"), new IntWritable(1))
                .withOutput(new Text("world"), new IntWritable(1))
                .runTest();
    }
}
