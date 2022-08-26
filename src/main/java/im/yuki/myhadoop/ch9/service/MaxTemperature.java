package im.yuki.myhadoop.ch9.service;

import im.yuki.myhadoop.ch9.entity.YearTempPair;
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
 * @description 利用键值组合寻找最大气温值
 * 运行命令：
 * hadoop jar myhadoop-1.1.0.jar im.yuki.myhadoop.ch9.service.MaxTemperature \
 * /user/longkun/myhadoop/ch9/max_temperature/temperature.txt \
 * /user/longkun/myhadoop/ch9/max_temperature/output
 */
public class MaxTemperature extends Configured implements Tool {

    public static class MaxTemperatureMapper extends Mapper<LongWritable, Text, YearTempPair, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int year = Integer.parseInt(line.substring(0, 4));
            int temperature = Integer.parseInt(line.substring(5));
            context.write(new YearTempPair(year, temperature), NullWritable.get());
        }
    }

    public static class MaxTemperatureReducer extends Reducer<YearTempPair, NullWritable, YearTempPair, NullWritable> {
        @Override
        protected void reduce(YearTempPair key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class YearPartitioner extends Partitioner<YearTempPair, NullWritable> {
        @Override
        public int getPartition(YearTempPair yearTempPair, NullWritable nullWritable, int i) {
            return Math.abs(yearTempPair.getYear() * 127) / i;
        }
    }

    public static class KeyComparator extends WritableComparator {

        protected KeyComparator() {
            super(YearTempPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            YearTempPair p1 = (YearTempPair) a;
            YearTempPair p2 = (YearTempPair) b;

            // 按年升序，按气温值降序
            return p1.compareTo(p2);
        }
    }

    public static class GroupComparator extends WritableComparator {

        protected GroupComparator() {
            super(YearTempPair.class, true);
        }

        // 将同一年的气温值送到同一个 reducer 处理
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            YearTempPair p1 = (YearTempPair) a;
            YearTempPair p2 = (YearTempPair) b;

            return Integer.compare(p1.getYear(), p2.getYear());
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");
        Job job = Job.getInstance(configuration, "MaxTemperatureJob");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setPartitionerClass(YearPartitioner.class);
        job.setCombinerKeyGroupingComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setOutputKeyClass(YearTempPair.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperature(), args);
        System.exit(exitCode);
    }
}
