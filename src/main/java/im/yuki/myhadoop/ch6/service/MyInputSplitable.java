package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/27 10:50 PM
 * @description 自定义文件分割
 */
public class MyInputSplitable extends TextInputFormat {

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

}
