package im.yuki.myhadoop.ch8.service;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/2 10:14 PM
 * @description 按气象站 id 输出多个文件
 */
public class MultiOutputReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
