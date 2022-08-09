package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/16 10:13 AM
 * @description 打印 hadoop 配置
 */
public class ConfigurationPrinter extends Configured implements Tool {

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");
        Configuration.addDefaultResource("yarn-site.xml");
    }

    public int run(String[] strings) {
        Configuration conf = getConf();
        for (Map.Entry<String, String> entry : conf) {
            String format = String.format("[%s=%s]\n", entry.getKey(), entry.getValue());
            System.out.println(format);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ConfigurationPrinter(), args);
        System.exit(exitCode);
    }
}
