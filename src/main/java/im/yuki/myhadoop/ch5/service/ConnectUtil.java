package im.yuki.myhadoop.ch5.service;

import im.yuki.myhadoop.ch5.constant.FSConstant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.net.URI;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/10 2:54 PM
 * @description 连接 HDFS 工具类
 */
public class ConnectUtil {

    public static FileSystem connect() {
        FileSystem fileSystem = null;
        try {
            Configuration configuration = new Configuration();
            fileSystem = FileSystem.get(new URI(FSConstant.HDFS_URL), configuration);
        } catch (Exception e) {
            System.out.println("连接 HDFS 异常, 异常信息如下: ");
            e.printStackTrace();
        }

        return fileSystem;
    }
}
