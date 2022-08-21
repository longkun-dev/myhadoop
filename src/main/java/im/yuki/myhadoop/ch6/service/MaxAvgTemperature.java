package im.yuki.myhadoop.ch6.service;

import im.yuki.myhadoop.ch6.entity.StationTempRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:59 PM
 * @description 计算每个气象站每年某天最高气温平均值
 * <p>
 * 数据格式 00_20220722_0900_4 表示 00 气象站在 2022年7月22日上午9时 温度为 4 摄氏度
 * </p>
 * <p>
 * 执行指令
 * hadoop jar target/myhadoop-1.1.0.jar im.yuki.myhadoop.ch6.service.MaxAvgTemperature \
 * /user/longkun/myhadoop/ch6/max_avg_temeperature/temperature.txt \
 * /user/longkun/myhadoop/ch6/max_avg_temeperature/output1 \
 * /user/longkun/myhadoop/ch6/max_avg_temeperature/output2
 */
public class MaxAvgTemperature extends Configured implements Tool {

    public static class MaxAvgTemperatureMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final StationTempRecord record = new StationTempRecord();

        enum InvalidLineCount {
            INVALID_LINE_COUNT
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // 过滤掉分隔用的行
            if (line.length() < 17) {
                System.err.println("无效行，值为: " + line);
                context.setStatus("检测到无效的行，请查看日志");
                // 无效行计数器加 1
                context.getCounter(MaxAvgTemperatureMapper1.InvalidLineCount.INVALID_LINE_COUNT).increment(1);
                return;
            }
            record.parse(line);
            context.write(new Text(record.getStation() + "_" + record.getDate()), new IntWritable(record.getTemp()));
        }
    }

    public static class MaxAvgTemperatureReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int maxTemp = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                int temp = value.get();
                if (temp > maxTemp) {
                    maxTemp = temp;
                }
            }

            context.write(key, new IntWritable(maxTemp));
        }
    }

    public static class MaxAvgTemperatureMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] pair = line.split("\\s");
            String key1 = pair[0];
            String newKey = key1.substring(0, 2) + "_" + key1.substring(7);
            context.write(new Text(newKey), new IntWritable(Integer.parseInt(pair[1])));
        }
    }

    public static class MaxAvgTemperatureReducer2 extends Reducer<Text, IntWritable, Text, FloatWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            float averageTemp;
            int sum = 0;
            int count = 0;
            for (IntWritable value : values) {
                sum += value.get();
                count++;
            }
            averageTemp = BigDecimal.valueOf(sum).divide(BigDecimal.valueOf(count), RoundingMode.HALF_UP)
                    .setScale(2, BigDecimal.ROUND_HALF_UP).floatValue();
            context.write(key, new FloatWritable(averageTemp));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        // 参数判断
        if (args.length < 3) {
            System.out.println("请至少指定 1 个输入参数和 2 个输出参数");
            System.exit(0);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath1 = new Path(args[1]);
        Path outputPath2 = new Path(args[2]);

        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "root.longkun.dev");

        Job job1 = Job.getInstance(configuration, "MaxAvgTempJob1");
        job1.setJarByClass(getClass());

        job1.setMapperClass(MaxAvgTemperatureMapper1.class);
        job1.setReducerClass(MaxAvgTemperatureReducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath1);


        Job job2 = Job.getInstance(configuration, "MaxAvgTempJob2");
        job2.setJarByClass(getClass());

        job2.setMapperClass(MaxAvgTemperatureMapper2.class);
        job2.setReducerClass(MaxAvgTemperatureReducer2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, outputPath1);
        FileOutputFormat.setOutputPath(job2, outputPath2);

        return handleJobChain(job1, job2, "MaxAvgTempChain") ? 0 : 1;
    }

    public static boolean handleJobChain(Job job1, Job job2, String chainName) throws IOException {
        ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());
        controlledJob1.setJob(job1);

        ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());
        controlledJob2.setJob(job2);

        // 设置任务 2 依赖于 任务 1
        controlledJob2.addDependingJob(controlledJob1);

        JobControl jc = new JobControl(chainName);
        jc.addJob(controlledJob1);
        jc.addJob(controlledJob2);

        Thread jcThread = new Thread(jc);
        jcThread.start();
        while (true) {
            if (jc.allFinished()) {
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();
                return true;
            }
            if (jc.getFailedJobList().size() > 0) {
                System.out.println(jc.getFailedJobList());
                jc.stop();
                return false;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new MaxAvgTemperature(), args);
        System.exit(exitCode);
    }
}
