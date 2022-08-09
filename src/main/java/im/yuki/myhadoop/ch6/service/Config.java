package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/16 12:29 AM
 * @description 从 xml 配置文件读取配置
 */
public class Config {

    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.addResource("configuration-1.xml");
        configuration.addResource("configuration-2.xml");
        String color = configuration.get("color");
        int size = configuration.getInt("size", 0);
        String colorSize = configuration.get("color-size");
        System.out.println("color: " + color);
        System.out.println("size: " + size);
        System.out.println("colorSize: " + colorSize);
    }
}
