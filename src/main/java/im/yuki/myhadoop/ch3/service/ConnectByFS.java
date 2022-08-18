package im.yuki.myhadoop.ch3.service;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/10 2:33 PM
 * @description 通过 FileSystem API 连接 Hadoop
 */
public class ConnectByFS {

    public static void connect() {
        try {
            Configuration configuration = new Configuration();
            String filePath = "/user/longkun/myhadoop/ch3/textfiles/temperature.txt";
            FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop00:9000"),
                    configuration);
            FSDataInputStream inputStream = fileSystem.open(new Path(filePath));
            IOUtils.copy(inputStream, System.out);
            long pos = inputStream.getPos();
            System.out.println("pos: " + pos);

            byte[] part = new byte[6];
            // 重要 读取之前将位置偏移置 0
            inputStream.seek(0);
            inputStream.read(part, 0, 6);
            String partResult = new String(part);
            System.out.println("partResult: " + partResult);

            IOUtils.closeQuietly(inputStream);
        } catch (Exception e) {
            System.out.println("出错了呢");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ConnectByFS.connect();
    }
}
