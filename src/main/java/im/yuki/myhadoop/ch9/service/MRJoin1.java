package im.yuki.myhadoop.ch9.service;

import im.yuki.myhadoop.ch9.entity.NationSpecialtyPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/7 7:59 PM
 * @description MapReduce 实现两张表的连接，一个源的数据排列在另一个源的数据之前以提高效率
 * 运行命令：
 * hadoop jar myhadoop-1.1.0.jar im.yuki.myhadoop.ch9.service.MRJoin \
 * /user/longkun/myhadoop/ch9/join/nation.txt \
 * /user/longkun/myhadoop/ch9/join/specialty.txt \
 * /user/longkun/myhadoop/ch9/join/output
 */
public class MRJoin1 extends Configured implements Tool {

    public static class NationMapper extends Mapper<LongWritable, Text, NationSpecialtyPair, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            NationSpecialtyPair nationSpecialtyPair = new NationSpecialtyPair(split[0], 0);
            context.write(nationSpecialtyPair, new Text(split[1]));
        }
    }

    public static class SpecialtyMapper extends Mapper<LongWritable, Text, NationSpecialtyPair, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            NationSpecialtyPair nationSpecialtyPair = new NationSpecialtyPair(split[1], 1);
            context.write(nationSpecialtyPair, new Text(split[0] + " " + split[2]));
        }
    }

    public static class KeyPartitioner extends Partitioner<NationSpecialtyPair, Text> {
        @Override
        public int getPartition(NationSpecialtyPair nationSpecialtyPair, Text text, int i) {
            return nationSpecialtyPair.hashCode() & Integer.MAX_VALUE % i;
        }
    }

    // 按照 code 升序、order 升序进行排序
    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(NationSpecialtyPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            NationSpecialtyPair p1 = (NationSpecialtyPair) a;
            NationSpecialtyPair p2 = (NationSpecialtyPair) b;
            return p1.compareTo(p2);
        }
    }

    // 同一个 code 下的记录进入同一个 iterator
    public static class GroupComparator extends WritableComparator {
        protected GroupComparator() {
            super(NationSpecialtyPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            NationSpecialtyPair p1 = (NationSpecialtyPair) a;
            NationSpecialtyPair p2 = (NationSpecialtyPair) b;
            return p1.getCode().compareTo(p2.getCode());
        }
    }

    public static class MRJoinReducer extends Reducer<NationSpecialtyPair, Text, Text, NullWritable> {
        @Override
        protected void reduce(NationSpecialtyPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            Text next = iterator.next();
            String nationName = next.toString();
            System.out.println("nationName: " + nationName);
            while (iterator.hasNext()) {
                Text line = iterator.next();
                String outVal = line.toString();
                System.out.println("outVal: " + outVal);
                context.write(new Text(nationName + " " + outVal), NullWritable.get());
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");

        Job job = Job.getInstance(configuration, "MRJoin1Job");
        job.setJarByClass(getClass());

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NationMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, SpecialtyMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // 如果没有设置 setMapOutputKeyClass 的话，就设置为 Map 端输出的 Key 类型
        job.setOutputKeyClass(NationSpecialtyPair.class);
        job.setOutputValueClass(Text.class);

        job.setReducerClass(MRJoinReducer.class);
        job.setPartitionerClass(KeyPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        job.setPriority(JobPriority.HIGH);
        job.setMaxReduceAttempts(3);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MRJoin1(), args);
        System.exit(exitCode);
    }
}
