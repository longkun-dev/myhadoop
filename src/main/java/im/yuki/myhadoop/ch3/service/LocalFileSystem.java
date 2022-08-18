package im.yuki.myhadoop.ch3.service;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/18 11:13 PM
 * @description 测试获取本地文件系统
 */
public class LocalFileSystem {

    public static void main(String[] args) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.getLocal(configuration);
        Path path = new Path("/Users/longkun/Documents/Java/myhadoop/files/ch2/max_temperature/temperature.txt");
        boolean exists = fileSystem.exists(path);
        System.out.println(exists);

        if (exists) {
            FSDataInputStream dataInputStream = fileSystem.open(path);
            IOUtils.copy(dataInputStream, System.out);
            IOUtils.closeQuietly(dataInputStream);
        }
    }
}
