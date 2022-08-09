package im.yuki.myhadoop.ch9.service;

import im.yuki.myhadoop.ch9.entity.IntPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/6 10:22 PM
 * @description 利用键值组合寻找最大值
 */
public class MaxSecondary extends Configured implements Tool {

    public static class MaxSecondaryMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int year = Integer.parseInt(line.substring(0, 4));
            int temperature = Integer.parseInt(line.substring(5));
            context.write(new IntPair(year, temperature), NullWritable.get());
        }
    }

    public static class MaxSecondaryReducer extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
        @Override
        protected void reduce(IntPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
        @Override
        public int getPartition(IntPair intPair, NullWritable nullWritable, int i) {
            return Math.abs(intPair.getFirst() * 127) / i;
        }
    }

    public static class KeyComparator extends WritableComparator {

        protected KeyComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair p1 = (IntPair) a;
            IntPair p2 = (IntPair) b;

            // 按年升序，按气温值降序
            return p1.compareTo(p2);
        }
    }

    public static class GroupComparator extends WritableComparator {

        protected GroupComparator() {
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntPair p1 = (IntPair) a;
            IntPair p2 = (IntPair) b;

            return Integer.compare(p1.getFirst(), p2.getFirst());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "dev");
        Job job = Job.getInstance(configuration, "keyPairJob");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxSecondaryMapper.class);
        job.setReducerClass(MaxSecondaryReducer.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setCombinerKeyGroupingComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxSecondary(), args);
        System.exit(exitCode);
    }
}
