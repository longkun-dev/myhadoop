package im.yuki.myhadoop.ch5.service;

import im.yuki.myhadoop.ch5.constant.FSConstant;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.PrintStream;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/20 5:04 PM
 * @description 写入 SequenceFile 对象
 */
public class SequenceFileWrite {

    public static void main(String[] args) throws IOException {
        String[] messages = new String[]{
                "1 -> hello world",
                "2 -> hello world",
                "3 -> hello world",
                "4 -> hello world"
        };

        Configuration configuration = new Configuration();

        String filePath = "/user/longkun/myhadoop/ch5/sequence_file_write/message";
        Path path = new Path(FSConstant.HDFS_URL + filePath);

        SequenceFile.Writer writer = null;
        IntWritable key = new IntWritable();
        Text value = new Text();
        try {
            // 《Hadoop权威指南中的方法已过时》
            SequenceFile.Writer.Option option1 = SequenceFile.Writer.file(path);
            SequenceFile.Writer.Option option2 = SequenceFile.Writer.keyClass(IntWritable.class);
            SequenceFile.Writer.Option option3 = SequenceFile.Writer.valueClass(Text.class);
            SequenceFile.Writer.Option option4 = SequenceFile.Writer.appendIfExists(true);
            writer = SequenceFile.createWriter(configuration, option1, option2, option3, option4);

            for (int i = 0; i < 400; i++) {
                key.set(i);
                value.set(messages[i % messages.length]);
                PrintStream log = System.out.format("[%d] \t %s \t %s", writer.getLength(), i, value);
                System.out.println(log);
                writer.append(key, value);
            }
        } finally {
            IOUtils.closeQuietly(writer);
        }
    }
}
