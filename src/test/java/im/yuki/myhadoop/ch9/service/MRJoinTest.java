package im.yuki.myhadoop.ch9.service;

import org.junit.Test;

import java.util.Arrays;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/7 6:11 PM
 * @description 连接测试类
 */
public class MRJoinTest {

    @Test
    public void test1() {
        String line = "JP Japan";
        String[] split = line.split(" ");
        System.out.println(Arrays.toString(split));
        System.out.println(split[0]);
        System.out.println(split[1]);
    }

    @Test
    public void test2() {
        String line = "Music JP 好听的日本轻音乐";
        String[] split = line.split(" ");
        System.out.println(split[0]);
        System.out.println(split[1]);
        System.out.println(split[2]);
    }

    @Test
    public void test3() {
        String replace = "001234".replace("00", "");
        System.out.println(replace);
    }

    @Test
    public void test4() {
        String line1 = "11Plane 美国波音公司设计制造的飞机";
        String line2 = "00America";
        String line11 = "11Fruit 台湾的热带水果";
        String line22 = "00Taiwan";

        System.out.println(line1.compareTo(line2));
        System.out.println(line11.compareTo(line22));

        System.out.println(Integer.compare(0, 1));

        System.out.println("hello".compareTo("hello"));
    }
}
