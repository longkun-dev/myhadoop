package im.yuki.myhadoop.ch9.service;

import im.yuki.myhadoop.ch9.entity.TempRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/6 7:03 PM
 * @description 将文件格式转为顺序问价 sequenceFile
 */
public class Convert2SeqFile extends Configured implements Tool {

    static class ConvertMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final TempRecord record = new TempRecord();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int val = Integer.parseInt(line);
            context.write(new IntWritable(val), value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "dev");
        Job job = Job.getInstance(configuration, "convert2seqFileJob");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ConvertMapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(3);

//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        SequenceFileOutputFormat.setCompressOutput(job, true);
//        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
//        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Convert2SeqFile(), args);
        System.exit(exitCode);
    }
}
