package im.yuki.myhadoop.ch8.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/22 10:46 PM
 * @description 将输出写到多个文件
 * 数据格式:
 * 一年级 张三
 * 一年级 李四
 * 二年级 王五
 * 二年级 马六
 *
 * 命令：
 * hadoop jar myhadoop-1.1.0.jar im.yuki.myhadoop.ch8.service.MultipleOutput \
 * /user/longkun/myhadoop/ch8/multiple_output/ \
 * /user/longkun/myhadoop/ch8/multiple_output/output
 */
public class MultipleOutput extends Configured implements Tool {

    public static class MultipleOutputMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String grade = line.substring(0, 6);
            String name = line.substring(7);
            context.write(new Text(grade), new Text(name));
        }
    }

    public static class MultipleOutputReducer extends Reducer<Text, Text, Text, Text> {

        private MultipleOutputs<Text, Text> multipleOutputs;

        @Override
        protected void setup(Context context) {
            this.multipleOutputs = new MultipleOutputs<>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                this.multipleOutputs.write(key, value, key.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            this.multipleOutputs.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        if (args.length < 2) {
            System.out.println("输入参数错误");
            System.exit(0);
        }

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");
        configuration.setInt("mapreduce.task.io.sort.mb", 100);

        Job job = Job.getInstance(configuration, "MultipleOutputJob");
        job.setJarByClass(getClass());

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(MultipleOutputMapper.class);
        job.setReducerClass(MultipleOutputReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MultipleOutput(), args);
        System.exit(exitCode);
    }
}
