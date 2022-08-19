package im.yuki.myhadoop.ch5.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/20 12:03 AM
 * @description 压缩从标输入中读取的数据，将其写到标准输出
 *
 * 运行命令：
 * echo "Text" | hadoop jar target/myhadoop-1.1.0.jar \
 * im.yuki.myhadoop.ch5.service.StreamCompressor \
 * org.apache.hadoop.io.compress.GzipCodec | gunzip
 */
public class StreamCompressor {

    public static void main(String[] args) throws Exception {
        Class<?> codecClassName = Class.forName(args[0]);
        Configuration configuration = new Configuration();
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClassName, configuration);

        CompressionOutputStream outputStream = codec.createOutputStream(System.out);
        IOUtils.copyBytes(System.in, outputStream, 4096, false);
        outputStream.finish();
    }
}
