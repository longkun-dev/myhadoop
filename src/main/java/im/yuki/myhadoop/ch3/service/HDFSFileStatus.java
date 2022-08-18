package im.yuki.myhadoop.ch3.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.net.URI;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/18 11:58 PM
 * @description FileSystem 类
 */
public class HDFSFileStatus {

    private static final String HDFS_ADDR = "hdfs://hadoop00:9000";

    private static final String FILE_PATH = "/user/longkun/myhadoop/ch3/textfiles/temperature.txt";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(HDFS_ADDR), configuration);
        Path path = new Path(FILE_PATH);
        FileStatus fileStatus = fileSystem.getFileStatus(path);

        System.out.println(fileStatus.getAccessTime());
        System.out.println(fileStatus.getModificationTime());
        System.out.println(fileStatus.getReplication());
        System.out.println(fileStatus.getLen());
        System.out.println(fileStatus.getGroup());
        System.out.println(fileStatus.getBlockSize());
        System.out.println(fileStatus.getOwner());
        System.out.println(fileStatus.getPermission());
        System.out.println(fileStatus.getClass());

        System.out.println("==============");

        Path path1 = new Path("/user/longkun/myhadoop/ch3/textfiles/");
        FileStatus[] listStatus = fileSystem.listStatus(path1, new MyPathFilter("[a-zA-Z]+.txt"));
        Path[] paths = FileUtil.stat2Paths(listStatus);
        for (Path path2 : paths) {
            System.out.println(path2);
        }
    }
}
