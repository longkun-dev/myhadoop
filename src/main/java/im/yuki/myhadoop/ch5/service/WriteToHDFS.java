package im.yuki.myhadoop.ch5.service;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.Progressable;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/10 2:58 PM
 * @description 向 HDFS 中写入数据
 */
public class WriteToHDFS {

    public static void status() {
        String path = "/user/longkun/tmp";
        Path path1 = new Path(path);
        FileSystem fileSystem = ConnectUtil.connect();

        try {
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path1, true);
            while (iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();
                System.out.println(fileStatus.getPath());
            }
            System.out.println("***********");

            FileStatus[] listStatus = fileSystem.listStatus(path1, path2 -> !path2.getName().endsWith(".log"));
            for (FileStatus status : listStatus) {
                System.out.println(status.getPath());
            }

            FileStatus fileStatus = fileSystem.getFileStatus(path1);
            System.out.println(fileStatus.getAccessTime());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
        } catch (Exception e) {
            System.out.println("错误信息: ");
            e.printStackTrace();
        }
    }

    public static void mkdir() {
        String path = "/user/longkun/tmp";
        Path path1 = new Path(path);

        FileSystem fileSystem = ConnectUtil.connect();
        try {
            boolean exists = fileSystem.exists(path1);
            if (exists) {
                System.out.println("文件目录已存在，跳过");
            } else {
                System.out.println("文件目录不存在，新建");

                boolean result = fileSystem.mkdirs(path1);
                System.out.println(result ? "新建成功" : "新建失败");
            }
        } catch (Exception e) {
            System.out.println("错误: ");
            e.printStackTrace();
        }
    }

    public static void write() {

        String content = "I love Norway!!\nI want to have a travel to Norway!!\n";
        String path = "/user/longkun/files/love_norway.txt";

        FileSystem fileSystem = ConnectUtil.connect();

        try {
            Path path1 = new Path(path);
            // 检查文件路径是否存在
            boolean exists = fileSystem.exists(path1);
            if (!exists) {
                System.out.println("文件路径不存在，将创建");
            } else {
                System.out.println("文件路径已存在");
            }
            FSDataOutputStream fsDataOutputStream = fileSystem.create(path1, () -> System.out.println("."));
            fsDataOutputStream.write(content.getBytes(), 0, content.length());
            // 强制将数据写入 datanode 内存中，性能开销大
            fsDataOutputStream.hflush();
            // 强制将数据写入 datanode 磁盘，新能开销大
            fsDataOutputStream.hsync();
            IOUtils.closeQuietly(fsDataOutputStream);

            // 暂停等待上面的操作完成
            Thread.sleep(1500);

            FSDataOutputStream append = fileSystem.append(path1);
            append.write("I will go!\n".getBytes(), 0, 10);

            IOUtils.closeQuietly(append);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
