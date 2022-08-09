package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/30 11:17 PM
 * @description 合并小文件 reducer
 */
public class MergeReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder content = new StringBuilder();
        for (Text value : values) {
            content.append(value.toString());
        }

        int rowNum = 0;
        int length = content.length();
        System.out.println("文件共有字符数量：" + length);

        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < length; i++) {
            char c = content.charAt(i);
            if (c < 65 || c > 122) {
                continue;
            }
            buffer.append(c);
            if (buffer.length() % 10 == 0) {
                System.out.println("第 " + rowNum + "行已处理完毕");
                rowNum = rowNum + 1;
                context.write(new IntWritable(rowNum), new Text(buffer.toString()));
                // 每生成一行之后重新开始
                buffer = new StringBuilder();
            } else if (i == length - 1) {
                // 处理最后一行不满 10 个字符的情况
                context.write(new IntWritable(rowNum), new Text(buffer.toString()));
            }
        }
    }
}
