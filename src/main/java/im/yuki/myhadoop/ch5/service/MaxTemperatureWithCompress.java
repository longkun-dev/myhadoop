package im.yuki.myhadoop.ch5.service;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/20 3:09 PM
 * @description 查找最高气温并对作业输出进行压缩
 * 命令：
 * hadoop jar myhadoop-1.1.0.jar im.yuki.myhadoop.ch5.service.MaxTemperatureWithCompress \
 * /user/longkun/myhadoop/ch5/max_temperature_compress/temperature.txt \
 * /user/longkun/myhadoop/ch5/max_temperature_compress/output
 */
public class MaxTemperatureWithCompress extends Configured implements Tool {

    public static class MaxTemperatureCompressMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String year = line.substring(0, 4);
            String temperatureStr = line.substring(9);
            if (StringUtils.isBlank(temperatureStr)) {
                System.out.println("气温值缺失：" + line);
                return;
            }
            int temperature = Integer.parseInt(temperatureStr);
            context.write(new Text(year), new IntWritable(temperature));
        }
    }

    public static class MaxTemperatureCompressReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxTemperature = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                int temperature = value.get();
                if (temperature > maxTemperature) {
                    maxTemperature = temperature;
                }
            }
            context.write(key, new IntWritable(maxTemperature));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("请指定输入和输出文件位置");
            System.exit(0);
        }

        Configuration configuration = new Configuration();
        // 指定任务提交的队列
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");
        // 指定 map shuffle 大小，避免溢出
        configuration.setInt("mapreduce.task.io.sort.mb", 150);
        // 压缩 map 端输出
        configuration.setBoolean(Job.MAP_OUTPUT_COMPRESS, true);
        configuration.setClass(Job.MAP_OUTPUT_COMPRESS, GzipCodec.class, CompressionCodec.class);

        Job job = Job.getInstance(configuration, "MaxTemperatureCompressJob");
        job.setJarByClass(getClass());

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // 指定压缩方式，压缩输出文件
        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

        job.setMapperClass(MaxTemperatureCompressMapper.class);
        job.setCombinerClass(MaxTemperatureCompressReducer.class);
        job.setReducerClass(MaxTemperatureCompressReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxTemperatureWithCompress(), args);
        System.exit(exitCode);
    }
}
