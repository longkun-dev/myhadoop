package im.yuki.myhadoop.ch8.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
 * @date 2022/8/2 10:23 PM
 * @description 根据气象站 id 输出分区文件
 */
public class MultiOutput extends Configured implements Tool {

    public static class MultiOutputMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // 每行数据前两位为气象站 id
            String stationId = line.substring(0, 2);
            context.write(new Text(stationId), value);
        }
    }

    public static class MultiOutputPartitioner extends Partitioner<Text, Text> {

        /**
         * 这种方法可以实现根据气象站 id 对输出文件进行分区，
         * 缺点：必须事先知道气象站 id 的个数
         * 可能导致分区数不均匀，从而使很多 reducer 做很少的工作
         *
         * @param key   key
         * @param value value
         * @param i     分区数
         * @return 分区数
         */
        @Override
        public int getPartition(Text key, Text value, int i) {
            String k = key.toString();
            if ("01".equals(k)) {
                return 0;
            } else if ("02".equals(k)) {
                return 1;
            } else if ("03".equals(k)) {
                return 2;
            } else {
                return 3;
            }
        }
    }

    public static class MultiOutputReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

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
        int exitCode = ToolRunner.run(new MultiOutput(), args);
        System.exit(exitCode);
    }
}
