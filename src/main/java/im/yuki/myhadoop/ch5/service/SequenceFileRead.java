package im.yuki.myhadoop.ch5.service;

import im.yuki.myhadoop.ch5.constant.FSConstant;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.PrintStream;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/20 5:30 PM
 * @description 读取 SequenceFile
 */
public class SequenceFileRead {

    public static void main(String[] args) {
        String filePath = "/user/longkun/myhadoop/ch5/sequence_file_write/message";
        Path path = new Path(FSConstant.HDFS_URL + filePath);

        Configuration configuration = new Configuration();

        SequenceFile.Reader reader = null;

        try {
            SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(path);
            SequenceFile.Reader.Option option2 = SequenceFile.Reader.start(0L);
            SequenceFile.Reader.Option option3 = SequenceFile.Reader.bufferSize(1024);
//            SequenceFile.Reader.Option option3 = SequenceFile.Reader.stream();
            reader = new SequenceFile.Reader(configuration, option1, option2, option3);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                // 是否是同步点
                String syncSeen = reader.syncSeen() ? "**" : "";
                PrintStream msg = System.out.format("[%d %s] %s %s", position, syncSeen, key, value);
                System.out.println(msg);
                position = reader.getPosition();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeQuietly(reader);
        }
    }
}
