package im.yuki.myhadoop.ch8.service;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/2 10:58 PM
 * @description 使用 MultiOutput 根据气象站 id 输出多个文件
 */
public class MultiOutput1Reducer extends Reducer<Text, Text, Text, Text> {

    private MultipleOutputs<Text, Text> multipleOutputs;

    @Override
    protected void setup(Context context) {
        this.multipleOutputs = new MultipleOutputs<>(context);
    }

    /**
     * 根据 mapper 任务产生的 key 将结果输出到多个文件中
     * 结果的文件名：key-r-00000 key-r-00001
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            this.multipleOutputs.write(key, value, key.toString());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        this.multipleOutputs.close();
    }
}
