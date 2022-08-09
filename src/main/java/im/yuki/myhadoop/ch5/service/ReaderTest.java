package im.yuki.myhadoop.ch5.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.net.URI;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/15 11:32 PM
 * @description SequenceFile 读取测试
 */
public class ReaderTest {

    public static final String HOST = "hdfs://localhost:9000";

    public static final String FILE_PATH = HOST + "/user/longkun/files/test.txt";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HOST), configuration);
        Path path = new Path(FILE_PATH);

        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, path, configuration);

        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
        long position = reader.getPosition();
        while (reader.next(key, value)) {
            String syncSeen = reader.syncSeen() ? "*" : "";
            String result = String.format("[%s\t%s\t%s\t%s]\n", position, syncSeen, key, value);
            System.out.println(result);
            position = reader.getPosition();
        }

        IOUtils.closeStream(reader);
    }
}
