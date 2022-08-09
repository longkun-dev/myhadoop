package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/30 11:20 PM
 * @description 合并小文件
 */
public class MergeDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("请指定小文件所在目录和合并后文件输出目录");
            return 0;
        }

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "dev");

        Job job = Job.getInstance(configuration, "mergeSmallFilesJob");
        job.setJarByClass(getClass());

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(MergeReducer.class);

        job.setInputFormatClass(MergeFileFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MergeDriver(), args);
        System.exit(exitCode);
    }
}
