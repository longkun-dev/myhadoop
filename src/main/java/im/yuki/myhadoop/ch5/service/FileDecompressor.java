package im.yuki.myhadoop.ch5.service;

import im.yuki.myhadoop.ch5.constant.FSConstant;
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
 * @description 根据文件扩展名选取合适的 codec 进行解压
 */
public class FileDecompressor {
    public static void main(String[] args) throws Exception {
        String FILE_PATH = "/user/longkun/myhadoop/ch5/file_decompress/hello.txt.gz";

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(FSConstant.HDFS_URL), configuration);


        Path inputPath = new Path(FILE_PATH);

        System.out.println(fileSystem.getFileChecksum(inputPath));

        CompressionCodecFactory codecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec codec = codecFactory.getCodec(inputPath);
        if (codec == null) {
            System.out.println("no codec found");
            System.exit(1);
        }

        String outputURI = CompressionCodecFactory.removeSuffix(FILE_PATH, codec.getDefaultExtension());

        InputStream inputStream = null;
        OutputStream outputStream = null;
        try {
            inputStream = codec.createInputStream(fileSystem.open(inputPath));
            outputStream = fileSystem.create(new Path(outputURI));
            IOUtils.copyBytes(inputStream, outputStream, configuration);
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }
    }
}
