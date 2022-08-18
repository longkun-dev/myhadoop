package im.yuki.myhadoop.ch3.service;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

import java.io.InputStream;
import java.net.URL;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/10 2:17 PM
 * @description 通过 URL 连接 HDFS 文件系统
 */
public class ConnectHDFS {

    static {
        // 每个 Java 虚拟机只能调用一次
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void ope() {
        InputStream inputStream;
        try {
            inputStream = new URL("hdfs://hadoop00:9000/user/longkun/myhadoop/ch3/textfiles/temperature.txt")
                    .openStream();
            IOUtils.copy(inputStream, System.out, 2048);
            IOUtils.closeQuietly(inputStream);
        } catch (Exception e) {
            System.out.println("处理异常");
        }
    }

    public static void main(String[] args) {
        ConnectHDFS.ope();
    }
}
