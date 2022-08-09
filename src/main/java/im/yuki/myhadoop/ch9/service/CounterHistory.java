package im.yuki.myhadoop.ch9.service;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/4 11:48 PM
 * @description 查看历史记录中计数器的值
 */
public class CounterHistory extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        String jobID = args[0];
        Cluster cluster = new Cluster(getConf());
        Job job = cluster.getJob(JobID.forName(jobID));
        if (job == null) {
            System.err.println("没有找到该任务");
            return -1;
        }

        if (!job.isComplete()) {
            System.err.println("该任务还没有结束");
            return -1;
        }

        System.out.printf("完成时间: %s\n", job.getFinishTime());
        System.out.printf("日志链接: %s\n", job.getHistoryUrl());
        System.out.printf("任务名称: %s\n", job.getJobName());
        System.out.printf("任务状态: %s\n", job.getJobState());
        System.out.printf("优先级: %s\n", job.getPriority());

        Counters counters = job.getCounters();
        Counter counter = counters.findCounter(CounterMapper.Length.LENGTH_THAN_5_CNT);
        long value = counter.getValue();

        long value1 = counters.findCounter("DynamicCounter", "allLineCount").getValue();

        System.out.println("LENGTH_THAN_5_CNT: " + value);
        System.out.println("allLineCount: " + value1);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CounterHistory(), args);
        System.exit(exitCode);
    }
}
