package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 12:50 AM
 * @description 单词数量统计驱动类测试
 */
public class WordCountDriverTest {

    @Test
    public void testWordCountDriver() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:/Users/longkun/Documents/Java/myhadoop/");
        configuration.set("mapreduce.framework.name", "local");
        configuration.setInt("mapreduce.task.io.sort.mb", 1);

        Path inPath = new Path("files/word_count/word.txt");
        Path outPath = new Path("files/word_count/output");

        FileSystem fileSystem = FileSystem.getLocal(configuration);
        fileSystem.delete(outPath, true);

        WordCountDriver driver = new WordCountDriver();
        driver.setConf(configuration);
        int exitCode = driver.run(new String[]{
                inPath.toString(),
                outPath.toString()
        });
        System.exit(exitCode);
    }

}
