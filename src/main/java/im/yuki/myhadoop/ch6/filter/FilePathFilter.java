package im.yuki.myhadoop.ch6.filter;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/20 11:40 PM
 * @description 文件类型过滤
 */
public class FilePathFilter implements PathFilter {

    private final String regex;

    public FilePathFilter(String regex) {
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
}
