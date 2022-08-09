package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/31 12:34 AM
 * @description key-value 输入格式
 */
public class MyKeyValueInputFormat extends KeyValueTextInputFormat {

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }


}
