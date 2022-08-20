package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.Map;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/20 11:08 PM
 * @description 实现 Tool 接口运行应用程序
 */
public class ParseConfigurationWithTool extends Configured implements Tool {

    private static final Configuration configuration = new Configuration();

    static {
        configuration.addResource("ch6/config/configuration-1.xml");
        configuration.addResource("ch6/config/configuration-2.xml");
    }

    @Override
    public int run(String[] args) {
        for (Map.Entry<String, String> entry : configuration) {
            System.out.println(entry.getKey() + " -> " + entry.getValue());
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new ParseConfigurationWithTool(), args);
        System.exit(exitCode);
    }
}
