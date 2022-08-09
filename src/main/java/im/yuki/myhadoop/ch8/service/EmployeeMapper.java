package im.yuki.myhadoop.ch8.service;

import im.yuki.myhadoop.ch8.entity.Employee;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/31 12:48 AM
 * @description mapper 类
 */
public class EmployeeMapper extends MapReduceBase implements Mapper<LongWritable, Employee, LongWritable, Text> {

    @Override
    public void map(LongWritable longWritable,
                    Employee employee,
                    OutputCollector<LongWritable, Text> outputCollector,
                    Reporter reporter) throws IOException {
        String recordLine = employee.getNo() + " "
                + employee.getName() + " "
                + employee.getSex() + " "
                + employee.getAge();
        outputCollector.collect(longWritable, new Text(recordLine));
    }
}
