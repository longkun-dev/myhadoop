package im.yuki.myhadoop.ch5.service;

import im.yuki.myhadoop.ch6.entity.TempRecord;
import im.yuki.myhadoop.ch6.service.TempMapper;
import im.yuki.myhadoop.ch6.service.TempReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/18 11:27 PM
 * @description Mapper 单元测试
 */
public class MapperTest {

    // MapDriver 引错类，一直报错，有两个 MapDriver，注意
    @Test
    public void processTest() throws IOException {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new TempMapper())
                .withInput(new LongWritable(0), new Text("2022010110"))
                .withOutput(new Text("2022"), new IntWritable(10))
                .runTest();
    }

    @Test
    public void processRecord() {
        TempRecord record = new TempRecord();
        record.parse("2022010110");
        System.out.println(record);
    }

    @Test
    public void reducerTest() throws IOException {
        List<IntWritable> tempList = new ArrayList<>();
        tempList.add(new IntWritable(10));
        tempList.add(new IntWritable(15));
        tempList.add(new IntWritable(20));

        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new TempReducer())
                .withInput(new Text("2022"), tempList)
                .withOutput(new Text("2022"), new IntWritable(20))
                .runTest();
    }
}
