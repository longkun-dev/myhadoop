package im.yuki.myhadoop.ch9.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
 * @date 2022/8/4 11:28 PM
 * @description 自定义计数器
 *
 * 数据格式：
 * ahel
 * ahell
 * ahelloo
 * ahellooo
 * ahelloooo
 * ahelloooooo
 * ahellooooooo
 *
 * 运行：
 * hadoop jar myhadoop-1.1.0.jar im.yuki.myhadoop.ch9.service.MyCounter \
 * /user/longkun/myhadoop/ch9/my_counter \
 * /user/longkun/myhadoop/ch9/my_counter/output
 */
    public class MyCounter extends Configured implements Tool {

    public static class CounterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        enum Length {
            /**
             * 行长度超过 5 个字符的计数器
             */
            LENGTH_THAN_5_CNT,

            /**
             * 行长度超过 8 个字符的计数器
             */
            LENGTH_THAN_8_CNT,

            /**
             * 其他的统计计数器
             */
            OTHER_CNT
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            int length = line.length();
            if (length > 8) {
                context.getCounter(CounterMapper.Length.LENGTH_THAN_8_CNT).increment(1);
            } else if (length > 5) {
                context.getCounter(CounterMapper.Length.LENGTH_THAN_5_CNT).increment(1);
            } else {
                context.getCounter(CounterMapper.Length.OTHER_CNT).increment(1);
            }

            // 动态计数器
            context.getCounter("DynamicCounter", "allLineCount").increment(1);

            context.write(new IntWritable(length), value);
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");
        Job job = Job.getInstance(configuration, "counterJob");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CounterMapper.class);
        job.setReducerClass(Reducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MyCounter(), args);
        System.exit(exitCode);
    }
}
