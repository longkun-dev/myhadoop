package im.yuki.myhadoop.ch3.service;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/19 12:15 AM
 * @description 过滤文件
 */
public class MyPathFilter implements PathFilter {

    private final String regex;

    public MyPathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        // 过滤出满足条件的文件
        return !path.toString().matches(regex);
    }
}
