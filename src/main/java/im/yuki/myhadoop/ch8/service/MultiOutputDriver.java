package im.yuki.myhadoop.ch8.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/2 10:23 PM
 * @description 根据气象站 id 输出分区文件
 */
public class MultiOutputDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("请指定输入文件路径及结果输出路径");
            System.exit(0);
        }

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "dev");
        Job job = Job.getInstance(configuration, "outputPartitionJob");
        job.setJarByClass(getClass());

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(MultiOutputMapper.class);
        job.setPartitionerClass(MultiOutputPartitioner.class);
        job.setReducerClass(MultiOutputReducer.class);

        // 有多少个气象站 id 需要手动设置 reduce 任务数
        job.setNumReduceTasks(4);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultiOutputDriver(), args);
        System.exit(exitCode);
    }
}
