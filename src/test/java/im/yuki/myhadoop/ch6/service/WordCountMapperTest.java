package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 12:27 AM
 * @description 测试字符串分割
 */
public class WordCountMapperTest {

    public static void main(String[] args) {
        String str = "hello world! nihao ba.a";
        String[] split = str.split("!|\\s|\\.");
        System.out.println(Arrays.toString(split));
    }

    @Test
    public void test() throws IOException {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(1), new Text("hello world!i am your father."))
                .withOutput(new Text("hello"), new IntWritable(1))
                .withOutput(new Text("world"), new IntWritable(1))
                .withOutput(new Text("i"), new IntWritable(1))
                .withOutput(new Text("am"), new IntWritable(1))
                .withOutput(new Text("your"), new IntWritable(1))
                .withOutput(new Text("father"), new IntWritable(1))
                .runTest();
    }
}
