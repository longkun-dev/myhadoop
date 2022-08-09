package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/19 12:24 AM
 * @description mapreduce 任务驱动类
 */
public class TempDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 指定任务提交的队列
        configuration.set("mapreduce.job.queuename", "dev");
        Job job = Job.getInstance(configuration, "MapTemp");
        job.setJarByClass(getClass());

        // 添加多个文件输入路径
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileInputFormat.addInputPaths(job, new Path(args[0]).toString()
//                + "," + new Path(args[1]).toString());
//        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
        // 如果执行目录，则包含目录下的所有文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 设置文件过滤
        FileInputFormat.setInputPathFilter(job, MyPathFilter.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        CombineFileInputFormat.addInputPath();

        job.setMapperClass(TempMapper.class);
        job.setPartitionerClass(TempPartitioner.class);
        job.setNumReduceTasks(3);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new TempDriver(), args);
    }
}
