package im.yuki.myhadoop.ch5.service;

import im.yuki.myhadoop.ch5.constant.FSConstant;
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
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    }

    public static void ope() {
        InputStream inputStream;
        try {
            inputStream = new URL(FSConstant.HDFS_URL + "/user/longkun/files/hello.txt").openStream();
            IOUtils.copy(inputStream, System.out, 2048);
            IOUtils.closeQuietly(inputStream);
        } catch (Exception e) {
            System.out.println("处理异常");
        }
    }
}
