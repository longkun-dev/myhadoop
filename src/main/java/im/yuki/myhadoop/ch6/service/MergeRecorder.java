package im.yuki.myhadoop.ch6.service;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/30 10:42 PM
 * @description 合并小文件
 */
public class MergeRecorder extends RecordReader<NullWritable, Text> {

    private FileSplit fileSplit;

    private Configuration configuration;

    private Text value;

    private boolean processed;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() {
        if (this.processed) {
            return false;
        }

        FSDataInputStream fsDataInputStream = null;
        try {
            Path path = fileSplit.getPath();
            FileSystem fileSystem = FileSystem.get(configuration);
            fsDataInputStream = fileSystem.open(path);

            byte[] buffer = new byte[(int) fileSplit.getLength()];
            fsDataInputStream.read(buffer, 0, (int) fileSplit.getLength());
            this.value = new Text(new String(buffer));
            this.processed = true;
            return true;
        } catch (Exception e) {
            System.out.println("读取记录异常");
            e.printStackTrace();
            return false;
        } finally {
            IOUtils.closeQuietly(fsDataInputStream);
        }
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public Text getCurrentValue() {
        return value;
    }

    @Override
    public float getProgress() {
        return this.processed ? 1F : 0F;
    }

    @Override
    public void close() {
    }
}
