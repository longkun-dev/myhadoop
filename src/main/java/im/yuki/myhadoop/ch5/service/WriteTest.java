package im.yuki.myhadoop.ch5.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.net.URI;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/15 11:17 PM
 * @description Write 测试
 */
public class WriteTest {

    public static final String HOST = "hdfs://localhost:9000";

    public static final String FILE_PATH = HOST + "/user/longkun/files/test.txt";

    public static void main(String[] args) throws Exception {

        String[] data = new String[]{
                "Japan is a beautiful country,\n",
                "and there is so many delicious food in Japan,\n",
                "mountain FUJI is a famous in the world!\n",
                "I'll travel to Japan one day!\n\n"
        };

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HOST), configuration);
        Path path = new Path(FILE_PATH);

        IntWritable key = new IntWritable();
        Text value = new Text();
        SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem, configuration, path, IntWritable.class, Text.class);

        for (int i = 0; i < 100; i++) {
            key.set(100 - i);
            value.set(data[i % data.length]);
            String log = String.format("[%s\t%s\t%s]\n", writer.getLength(), key, value);
            System.out.println(log);
            writer.append(key, value);
        }

        IOUtils.closeStream(writer);
    }
}
