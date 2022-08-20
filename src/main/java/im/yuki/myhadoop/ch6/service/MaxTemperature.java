package im.yuki.myhadoop.ch6.service;

import im.yuki.myhadoop.ch6.entity.TempRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/19 12:24 AM
 * @description 查找最高气温值
 * 命令：
 * hadoop jar myhadoop-1.1.0.jar im.yuki.myhadoop.ch6.service.MaxTemperature \
 * /user/longkun/myhadoop/ch6/max_temperature \
 * /user/longkun/myhadoop/ch6/max_temperature/output
 */
public class MaxTemperature extends Configured implements Tool {

    public static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final TempRecord record = new TempRecord();

        enum Temperature {
            OVER_100
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 获取分片相关的信息
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            System.out.println("inputSplitSize: " + fileSplit.getLength());
            System.out.println("path: " + fileSplit.getPath());
            System.out.println("start: " + fileSplit.getStart());

            String line = value.toString();
            record.parse(line);
            if (record.getTemp() > 8) {
                System.err.println("温度超过 8 度: " + record.getTemp());
                context.setStatus("检测到可能错误的输入值，请查看日志");
                context.getCounter(Temperature.OVER_100).increment(1);
            }
            context.write(new Text(record.getYear()), new IntWritable(record.getTemp()));
        }
    }

    public static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int maxTemp = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxTemp = Math.max(maxTemp, value.get());
            }
            context.write(key, new IntWritable(maxTemp));
        }
    }

    public static class TempPartitioner extends Partitioner<Text, IntWritable> {

        // 按 10 年的数据分区
        @Override
        public int getPartition(Text text, IntWritable intWritable, int i) {
            String yearStr = text.toString();
            int year = Integer.parseInt(yearStr) - 2000;
            if (year < 10) {
                return 0;
            } else if (year < 20) {
                return 1;
            } else {
                return 2;
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        // 指定任务提交的队列
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");
        // 压缩 Map 端的输出
        configuration.setBoolean("mapreduce.map.output.compress", true);
        configuration.setClass("mapreduce.map.output.compress.codec", GzipCodec.class, CompressionCodec.class);
        Job job = Job.getInstance(configuration, "MapTemperature");
        job.setJarByClass(getClass());

        // 如果执行目录，则包含目录下的所有文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(TempMapper.class);
        job.setPartitionerClass(TempPartitioner.class);
        job.setNumReduceTasks(3);
        job.setReducerClass(TempReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new MaxTemperature(), args);
    }
}
