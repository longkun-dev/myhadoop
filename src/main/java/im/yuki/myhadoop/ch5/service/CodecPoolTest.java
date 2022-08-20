package im.yuki.myhadoop.ch5.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/20 12:33 AM
 * @description 如果使用原生代码库并且需要在应用中执行大量的压缩和解压缩操作，
 * 可以使用 CodecPool 支持反复使用压缩和解压缩，分摊创建对象的开销
 * 运行：
 * echo "HELLO" | hadoop jar target/myhadoop-1.1.0.jar im.yuki.myhadoop.ch5.service.CodecPoolTest \
 * org.apache.hadoop.io.compress.GzipCodec | gunzip
 */
public class CodecPoolTest {

    public static void main(String[] args) throws Exception {
        Class<?> codecClassName = Class.forName(args[0]);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClassName, new Configuration());

        Compressor compressor = null;
        try {
            compressor = CodecPool.getCompressor(codec);
            CompressionOutputStream outputStream = codec.createOutputStream(System.out, compressor);
            IOUtils.copyBytes(System.in, outputStream, 1024, false);
            outputStream.finish();
        } finally {
            CodecPool.returnCompressor(compressor);
        }
    }
}
