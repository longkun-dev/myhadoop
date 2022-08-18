package im.yuki.myhadoop.ch3.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.net.URI;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/15 11:17 PM
 * @description 向 HDFS 中写入数据
 */
public class Write2HDFS {

    public static final String HOST = "hdfs://hadoop00:9000";

    public static final String FILE_PATH = HOST + "/user/longkun/myhadoop/ch3/textfiles/write_content.txt";

    public static void main(String[] args) throws Exception {

        String message = "Japan is a beautiful country,\n" +
                "and there is so many delicious food in Japan,\n" +
                "mountain FUJI is famous in the world!\n" +
                "I'll travel to Japan one day!\n\n";

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HOST), configuration);
        Path path = new Path(FILE_PATH);

        // 判断文件是否存在
        boolean exists = fileSystem.exists(path);
        if (exists) {
            System.out.println("文件已存在，执行删除");
            // b: recursive
            fileSystem.delete(path, true);
        }

        FSDataOutputStream dataOutputStream = fileSystem.create(path);
        dataOutputStream.write(message.getBytes());

        IOUtils.closeStream(dataOutputStream);
    }
}
