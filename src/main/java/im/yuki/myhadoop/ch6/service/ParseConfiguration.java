package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/16 12:29 AM
 * @description 从 xml 配置文件读取配置
 */
public class ParseConfiguration {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.addResource("ch6/config/configuration-1.xml");
        configuration.addResource("ch6/config/configuration-2.xml");

        System.out.println("------ configuration -------");
        // 后面的配置会覆盖前面的配置，但是 final = true 的配置不会被覆盖
        System.out.println("color: " + configuration.get("color"));
        System.out.println("size: " + configuration.get("size"));
        System.out.println("color-size: " + configuration.get("color-size"));
    }
}
