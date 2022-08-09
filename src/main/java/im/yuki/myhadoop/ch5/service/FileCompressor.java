package im.yuki.myhadoop.ch5.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/14 8:15 PM
 * @description 文件压缩
 */
public class FileCompressor {
    public static void main(String[] args) throws Exception {
        String URL = "hdfs://localhost:9000/user/longkun/files/set_queue.sh.gz";
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(URL), configuration);


        Path path = new Path(URL);

        System.out.println(fileSystem.getFileChecksum(path));

        CompressionCodecFactory codecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = codecFactory.getCodec(path);
        if (codec == null) {
            System.out.println("no found");
            System.exit(1);
        }

        String outputURI = CompressionCodecFactory.removeSuffix(URL, codec.getDefaultExtension());

        InputStream inputStream = null;
        OutputStream outputStream = null;

        try {
            inputStream = codec.createInputStream(fileSystem.open(path));
            outputStream = fileSystem.create(new Path(outputURI));
            IOUtils.copyBytes(inputStream, outputStream, configuration);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }
    }
}
