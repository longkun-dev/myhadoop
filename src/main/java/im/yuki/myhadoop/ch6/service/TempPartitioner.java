package im.yuki.myhadoop.ch6.service;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/27 9:46 PM
 * @description
 */
public class TempPartitioner extends Partitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        int temp = intWritable.get();
        if (temp < 5) {
            return 1 % i;
        } else if (temp < 10) {
            return 2 % i;
        } else if (temp < 15) {
            return 3 % i;
        } else {
            return 4 % i;
        }
    }
}
