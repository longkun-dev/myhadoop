package im.yuki.myhadoop.ch9.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/7 6:08 PM
 * @description MapReduce 实现两张表的连接

 */
public class MRJoin extends Configured implements Tool {

    public static class NationMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            context.write(new Text(split[0]), new Text("0" + split[1]));
        }
    }

    public static class SpecialtyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            context.write(new Text(split[1]), new Text("1" + split[0] + " " + split[2]));
        }
    }

    public static class KeyPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text val, int i) {
            return key.hashCode() & Integer.MAX_VALUE % i;
        }
    }

    // 避免数据量大时 Reducer 内存不够用，在数据进入 Reducer 之前对同一个 key 下的 values 进行排序
    // 只能对键进行排序
    public static class KeyComparator extends Text.Comparator {
        public KeyComparator() {
            super();
        }

        @Override
        public int compare(Object a, Object b) {
            String val1 = (String) a;
            String val2 = (String) b;
            return val1.compareTo(val2);
        }
    }

    public static class MRJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> nationList = new ArrayList<>();
            List<String> specialtyList = new ArrayList<>();
            for (Text value : values) {
                String val = value.toString();
                if (val.startsWith("0")) {
                    nationList.add(val.replace("0", ""));
                } else {
                    specialtyList.add(val.replace("1", ""));
                }
            }

            for (String specialty : specialtyList) {
                context.write(new Text(nationList.get(0) + " " + specialty), NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");

        Job job = Job.getInstance(configuration, "MRJoinJob");
        job.setJarByClass(getClass());

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NationMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SpecialtyMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(MRJoinReducer.class);
        job.setPartitionerClass(KeyPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MRJoin(), args);
        System.exit(exitCode);
    }
}
