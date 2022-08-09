package im.yuki.myhadoop.ch6.service;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/30 11:51 AM
 * @description 读取整个文件
 */
public class WholeFileRecorder extends RecordReader<NullWritable, BytesWritable> {

    private FileSplit fileSplit;

    private Configuration configuration;

    private final BytesWritable value = new BytesWritable();

    private boolean processed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
        this.fileSplit = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() {
        if (this.processed) {
            return false;
        }
        FSDataInputStream inputStream = null;
        try {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fileSystem = file.getFileSystem(this.configuration);
            inputStream = fileSystem.open(file);
            IOUtils.readFully(inputStream, contents);
            this.value.set(contents, 0, contents.length);
            this.processed = true;
            return true;
        } catch (Exception e) {
            System.err.println("发生异常");
            e.printStackTrace();
            return false;
        } finally {
            IOUtils.closeQuietly(inputStream);
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() {
        return this.value;
    }

    @Override
    public float getProgress() {
        return this.processed ? 1.0F : 0.0F;
    }

    @Override
    public void close() {
        System.out.println("run close !");
    }
}
