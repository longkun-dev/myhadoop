package im.yuki.myhadoop.ch5.service;

import org.apache.hadoop.fs.ChecksumFileSystem;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/14 8:00 PM
 * @description CRC-32 循环冗余校验
 */
public class ChecksumTest {

    public static void main(String[] args) {
        double length = ChecksumFileSystem.getApproxChkSumLength(32);
        System.out.println(length);
    }
}
