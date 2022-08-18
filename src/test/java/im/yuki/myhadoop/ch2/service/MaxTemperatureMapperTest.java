package im.yuki.myhadoop.ch2.service;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/18 9:47 PM
 * @description
 */
public class MaxTemperatureMapperTest {

    @Test
    public void test1() {
        String line1 = "20220101_";
        String line2 = "20220101_1";
        String line3 = "20220101_10";
        System.out.println(line1.split("_").length);
        System.out.println(line2.substring(0, 8));
        System.out.println(line3.substring(9));
        System.out.println(line1.substring(9));
    }
}
