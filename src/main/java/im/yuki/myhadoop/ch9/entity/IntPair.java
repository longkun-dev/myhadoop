package im.yuki.myhadoop.ch9.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/6 10:25 PM
 * @description 案例 9.6 IntPair 类
 */
public class IntPair implements WritableComparable<IntPair> {

    private int first;

    private int second;

    public IntPair() {
    }

    public IntPair(int first, int second) {
        super();
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(IntPair o) {
        // 按年份升序、按气温值降序排序
        int compareFirst = Integer.compare(first, o.getFirst());
        if (compareFirst != 0) {
            return compareFirst;
        } else {
            return (-1) * Integer.compare(second, o.getSecond());
        }
    }

    @Override
    public int hashCode() {
        return first * 163 + second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(first);
        dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first = dataInput.readInt();
        second = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.first + " " + this.second;
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }
}
