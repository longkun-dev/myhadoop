package im.yuki.myhadoop.ch9.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/6 9:14 PM
 * @description 使用 TotalOrderPartitioner 进行全局排序
 *
 * hadoop jar myhadoop-1.1.0.jar im.yuki.myhadoop.ch9.service.GlobalSort \
 * /user/longkun/myhadoop/ch9/global_sort/part-r-00000 \
 * /user/longkun/myhadoop/ch9/global_sort/output1 \
 * /user/longkun/myhadoop/ch9/global_sort/output2
 */
public class GlobalSort extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");
        Job job = Job.getInstance(configuration, "globalSortJob");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(TotalOrderPartitioner.class);
        TotalOrderPartitioner.setPartitionFile(configuration, new Path(args[1]));

        // n + 1 分区的最小值大于 n 分区的最大值，全局有序
        // 该句必须放在 InputSampler.RandomSampler 前面，否则报错：Can't read partitions file
        job.setNumReduceTasks(3);

        InputSampler.RandomSampler<IntWritable, Text> randomSampler = new InputSampler.RandomSampler<>(1,
                5, 5);
        InputSampler.writePartitionFile(job, randomSampler);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GlobalSort(), args);
        System.exit(exitCode);
    }
}
