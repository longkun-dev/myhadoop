package im.yuki.myhadoop.ch6.entity;

import org.junit.Test;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:21 PM
 * @description 测试气温记录解析
 */
public class TempRecord1Test {

    private String dataLine1 = "00_20220722_0900_4";
    private String dataLine2 = "01_20220721_1800_10";

    @Test
    public void testParse() {
        System.out.println(dataLine1.substring(0, 2));
        System.out.println(dataLine1.substring(3, 11));
        System.out.println(dataLine1.substring(17));
        System.out.println(dataLine2.substring(0, 2));
        System.out.println(dataLine2.substring(3, 11));
        System.out.println(dataLine2.substring(17));
        TempRecord1 record1 = new TempRecord1();
        record1.parse(dataLine1);
        System.out.println(record1.getStation());
        System.out.println(record1.getDate());
        System.out.println(record1.getTemp());
    }

}
