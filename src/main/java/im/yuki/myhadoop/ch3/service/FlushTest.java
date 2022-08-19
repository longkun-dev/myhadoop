package im.yuki.myhadoop.ch3.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/19 9:26 PM
 * @description flush() 与 hflush() 方法
 */
public class FlushTest {

    private static final String HDFS_HOST = "hdfs://hadoop00:9000";

    private static final String FILE_PATH = "/user/longkun/myhadoop/ch3/textfiles/flush_test.txt";

    public static void main(String[] args) throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_HOST), configuration);
        Path path = new Path(FILE_PATH);
        if (fileSystem.exists(path)) {
            System.out.println("文件已存在，删除该文件");
            fileSystem.delete(path, true);
        }
        FSDataOutputStream dataOutputStream = fileSystem.create(path, true);
        dataOutputStream.write("test content".getBytes());
        dataOutputStream.flush();

        // 读取文件长度
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        System.out.println("flush 后文件长度: " + fileStatus.getLen());

        // flush() 将数据写入 datanode 的内存中，而不是磁盘
        dataOutputStream.hflush();
        FileStatus fileStatus1 = fileSystem.getFileStatus(path);
        System.out.println("hflush 后文件长度: " + fileStatus1.getLen());

        // hsync() 将数据写到磁盘
        dataOutputStream.hsync();
    }
}
