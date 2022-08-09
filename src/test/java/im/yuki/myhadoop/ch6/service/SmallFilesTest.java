package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/30 12:54 PM
 * @description 测试小文件合并成顺序文件
 */
public class SmallFilesTest {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "file:/Users/longkun/Documents/Java/myhadoop/");
        configuration.set("mapreduce.framework.name", "local");
        configuration.setInt("mapreduce.task.io.sort.mb", 1);

        Path inputPath = new Path("files/smallfiles/1.txt");
        Path outputPath = new Path("files/smallfiles/output");

        FileSystem fileSystem = FileSystem.getLocal(configuration);
        //　実行前に既存のフォルダを削除する
        fileSystem.delete(outputPath, true);

        SmallFileToSequenceFileConverter converter = new SmallFileToSequenceFileConverter();
        converter.setConf(configuration);
        int exitCode = converter.run(new String[]{inputPath.toString(), outputPath.toString()});
        System.exit(exitCode);
    }
}
