package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/30 11:20 PM
 * @description 合并小文件
 */
public class MergeFile extends Configured implements Tool {

    public static class MergeMapper extends Mapper<NullWritable, Text, IntWritable, Text> {

        @Override
        protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(1), value);
        }
    }

    public static class MergeReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder content = new StringBuilder();
            for (Text value : values) {
                content.append(value.toString());
            }

            int rowNum = 0;
            int length = content.length();
            System.out.println("文件共有字符数量：" + length);

            StringBuilder buffer = new StringBuilder();
            for (int i = 0; i < length; i++) {
                char c = content.charAt(i);
                if (c < 65 || c > 122) {
                    continue;
                }
                buffer.append(c);
                if (buffer.length() % 10 == 0) {
                    System.out.println("第 " + rowNum + "行已处理完毕");
                    rowNum = rowNum + 1;
                    context.write(new IntWritable(rowNum), new Text(buffer.toString()));
                    // 每生成一行之后重新开始
                    buffer = new StringBuilder();
                } else if (i == length - 1) {
                    // 处理最后一行不满 10 个字符的情况
                    context.write(new IntWritable(rowNum), new Text(buffer.toString()));
                }
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("请指定小文件所在目录和合并后文件输出目录");
            return 0;
        }

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "dev");

        Job job = Job.getInstance(configuration, "mergeSmallFilesJob");
        job.setJarByClass(getClass());

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(MergeMapper.class);
        job.setReducerClass(MergeReducer.class);

        job.setInputFormatClass(MergeFileFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MergeFile(), args);
        System.exit(exitCode);
    }
}
