package im.yuki.myhadoop.ch9.service;

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
 * @description 将文件格式转为顺序文件 sequenceFile
 * 数据格式：
 * 00_20220101 10
 * 00_20220102 12
 * 00_20220103 11
 * 01_20220101 22
 * 01_20220102 21
 * 01_20220103 22
 * 02_20220101 31
 * 02_20220102 32
 * 02_20220103 30
 *
 * 运行命令：
 * hadoop jar myhadoop-1.1.0.jar im.yuki.myhadoop.ch9.service.Convert2SeqFile \
 * /user/longkun/myhadoop/ch9/convert_to_seq_file/temperature.txt \
 * /user/longkun/myhadoop/ch9/convert_to_seq_file/output
 */
public class Convert2SeqFile extends Configured implements Tool {

    static class ConvertMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int val = Integer.parseInt(line.substring(12));
            context.write(new IntWritable(val), value);
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");
        Job job = Job.getInstance(configuration, "convert2seqFileJob");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(ConvertMapper.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Convert2SeqFile(), args);
        System.exit(exitCode);
    }
}
