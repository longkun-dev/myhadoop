package im.yuki.myhadoop.ch5.service;

import org.junit.Test;


/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/10 3:08 PM
 * @description 将数据写入 HDFS
 */
public class WriteToHDFSTest {

    @Test
    public void write() {
        WriteToHDFS.write();
    }

    @Test
    public void mkdir() {
        WriteToHDFS.mkdir();
    }

    @Test
    public void status() {
        WriteToHDFS.status();
    }
}
