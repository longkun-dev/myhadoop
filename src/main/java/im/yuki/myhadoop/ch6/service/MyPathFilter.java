package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/27 10:38 PM
 * @description 自定义文件过滤器
 */
public class MyPathFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        return path.getName().contains(".txt");
    }
}
