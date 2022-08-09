package im.yuki.myhadoop.ch9.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/6 9:14 PM
 * @description 全局排序
 */
public class GlobalSort extends Configured implements Tool {

    static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int val = Integer.parseInt(line);
            context.write(new IntWritable(val), value);
        }
    }

    static class SortPartitioner extends Partitioner<IntWritable, Text> {
        @Override
        public int getPartition(IntWritable intWritable, Text text, int partitionNum) {
            int val = intWritable.get();
            if (val > 10000) {
                return 0;
            } else if (val > 1000) {
                return 1;
            } else {
                return 2;
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "dev");
        Job job = Job.getInstance(configuration, "globalSortJob");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(SortMapper.class);
//        job.setPartitionerClass(SortPartitioner.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);

        InputSampler.RandomSampler<IntWritable, Text> randomSampler = new InputSampler.RandomSampler<>(1,
                100, 5);
        InputSampler.writePartitionFile(job, randomSampler);

//        job.setNumReduceTasks(3);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new GlobalSort(), args);
        System.exit(exitCode);
    }
}
