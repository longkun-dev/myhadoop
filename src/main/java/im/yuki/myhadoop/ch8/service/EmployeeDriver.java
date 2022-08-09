package im.yuki.myhadoop.ch8.service;

import im.yuki.myhadoop.ch8.entity.Employee;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/31 12:56 AM
 * @description employee driver 类
 */
public class EmployeeDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.job.queuename", "dev");
        Job job = Job.getInstance(configuration);
        job.setJarByClass(getClass());

        job.setInputFormatClass(DBInputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        DBConfiguration.configureDB(configuration, "com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/go_test",
                "root", "12345678");
        String[] fields = new String[]{"id", "name", "age", "birthday", "email"};
        DBInputFormat.setInput(job, Employee.class, "user", null, "id", fields);

//        job.setMapperClass(EmployeeMapper.class);
//        job.setReducerClass(IdentityReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new EmployeeDriver(), args);
        System.exit(exitCode);
    }
}
