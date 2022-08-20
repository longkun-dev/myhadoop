package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/22 10:20 PM
 * @description 驱动类测试
 */
public class MaxTemperatureTest {

    @Test
    public void test() throws Exception {
        Configuration configuration = new Configuration();
        // 本地作业运行器设置本地文件系统
        configuration.set("fs.defaultFS", "file:/Users/longkun/Documents/Java/myhadoop/");
        configuration.set("mapreduce.framework.name", "local");
        configuration.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("files/temp1.txt");
        Path output = new Path("files/output");

        FileSystem fileSystem = FileSystem.getLocal(configuration);
        fileSystem.delete(output, true);

        MaxTemperature driver = new MaxTemperature();
        driver.setConf(configuration);

        int exitCode = driver.run(new String[]{input.toString(), output.toString()});

        assert exitCode == 0;
    }
}
