package im.yuki.myhadoop.ch5.service;

import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.Arrays;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/14 8:32 PM
 * @description 序列化和反序列化
 */
public class SerializeTest {

    public static void main(String[] args) throws Exception {
        IntWritable intWritable = new IntWritable(16300);
        byte[] bytes = serialize(intWritable);
        System.out.println(Arrays.toString(bytes));

        System.out.println("------------");

        deserialize(intWritable, bytes);
        System.out.println(Arrays.toString(bytes));
        System.out.println(intWritable.get());
    }

    // 序列化
    public static byte[] serialize(Writable writable) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        writable.write(dataOutputStream);
        IOUtils.closeStream(out);
        IOUtils.closeStream(dataOutputStream);
        return out.toByteArray();
    }

    // 反序列化
    public static byte[] deserialize(Writable writable, byte[] bytes) throws IOException {
        InputStream inputStream = new ByteArrayInputStream(bytes);
        DataInputStream dataInputStream = new DataInputStream(inputStream);
        writable.readFields(dataInputStream);
        IOUtils.closeStream(inputStream);
        IOUtils.closeStream(dataInputStream);
        return bytes;
    }
}
