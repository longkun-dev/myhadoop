package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:59 PM
 * @description 某个气象站每年某天最高气温平均值驱动类
 * <p>
 * 执行指令
 * hadoop jar target/myhadoop-1.1.0.jar im.yuki.myhadoop.ch6.service.MaxAvgTempDriver \
 * /user/longkun/max_avg_temp/temp.txt \
 * /user/longkun/max_avg_temp/output1 \
 * /user/longkun/max_avg_temp/output2
 */
public class MaxAvgTempDriver extends Configured implements Tool {

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
        configuration.set("mapreduce.job.queuename", "dev");
        Job job1 = Job.getInstance(configuration, "MaxAvgTempJob1");
        job1.setJarByClass(getClass());

        job1.setMapperClass(MaxAvgTempMapper1.class);
        job1.setReducerClass(MaxAvgTempReducer1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputPath1);


        Job job2 = Job.getInstance(configuration, "MaxAvgTempJob2");
        job2.setJarByClass(getClass());

        job2.setMapperClass(MaxAvgTempMapper2.class);
        job2.setReducerClass(MaxAvgTempReducer2.class);

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
        int exitCode = ToolRunner.run(new MaxAvgTempDriver(), args);
        System.exit(exitCode);
    }
}
