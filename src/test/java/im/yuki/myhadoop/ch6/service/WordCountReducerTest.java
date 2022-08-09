package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.*;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 12:39 AM
 * @description 单词数量统计 reducer
 */
public class WordCountReducerTest {

    @Test
    public void test() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new WordCountReducer())
                .withInput(new Text("hello"), new ArrayList<IntWritable>() {{
                    add(new IntWritable(1));
                    add(new IntWritable(3));
                }})
                .withOutput(new Text("hello"), new IntWritable(4))
                .runTest();
    }

}
