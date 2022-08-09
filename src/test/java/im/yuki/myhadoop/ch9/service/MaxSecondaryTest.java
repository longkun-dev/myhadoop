package im.yuki.myhadoop.ch9.service;

import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/6 10:34 PM
 * @description MapReduce 的特性测试
 */
public class MaxSecondaryTest {

    @Test
    public void test1() {
        String line = "2020 1";
        System.out.println(line.substring(0, 4));
        System.out.println(line.substring(5));
    }

    @Test
    public void test2() throws Exception {
        String path = "files/max-second/test_data.txt";
        File file = new File(path);
        if (!file.exists()) {
            file.createNewFile();
        }
        OutputStream outputStream = new FileOutputStream(file);

        // 生成测试数据
        Random random = new Random();
        String line;
        for (int i = 0; i < 1000; i++) {
            int year = random.nextInt(23) + 2000;
            int temperature = random.nextInt(100);
            line = year + " " + temperature + "\n";
            outputStream.write(line.getBytes());
        }
        outputStream.flush();
        outputStream.close();

        System.out.println("完成！");
    }

    // 验证最大值正确与否
    @Test
    public void test3() throws Exception {
        String path = "files/max-second/test_data.txt";
        File file = new File(path);
        if (!file.exists()) {
            System.err.println("数据文件不存在");
            System.exit(0);
        }

        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = reader.readLine();
        String year;
        int temp;
        Map<String, Integer> map = new HashMap<>();
        while (line != null) {
            year = line.substring(0, 4);
            temp = Integer.parseInt(line.substring(5));

            Integer t = map.get(year);
            if (t == null) {
                map.put(year, temp);
            } else {
                map.put(year, Math.max(t, temp));
            }

            line = reader.readLine();
        }

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}
