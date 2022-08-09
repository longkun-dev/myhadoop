package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;


/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/30 10:48 PM
 * @description 合并小文件
 */
public class MergeFileFormat extends FileInputFormat<NullWritable, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        MergeRecorder mergeRecorder = new MergeRecorder();
        mergeRecorder.initialize(inputSplit, taskAttemptContext);
        return mergeRecorder;
    }
}
