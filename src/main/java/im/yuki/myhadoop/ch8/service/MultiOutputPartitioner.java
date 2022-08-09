package im.yuki.myhadoop.ch8.service;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/2 10:15 PM
 * @description 根据气象站 id 进行分区
 */
public class MultiOutputPartitioner extends Partitioner<Text, Text> {

    /**
     * 这种方法可以实现根据气象站 id 对输出文件进行分区，
     * 缺点：必须事先知道气象站 id 的个数
     * 可能导致分区数不均匀，从而使很多 reducer 做很少的工作
     *
     * @param key   key
     * @param value value
     * @param i     分区数
     * @return 分区数
     */
    @Override
    public int getPartition(Text key, Text value, int i) {
        String k = key.toString();
        if ("01".equals(k)) {
            return 0;
        } else if ("02".equals(k)) {
            return 1;
        } else if ("03".equals(k)) {
            return 2;
        } else {
            return 3;
        }
    }
}
