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
public class YearTempPair implements WritableComparable<YearTempPair> {

    private int year;

    private int temperature;

    public YearTempPair() {
    }

    public YearTempPair(int year, int temperature) {
        super();
        this.year = year;
        this.temperature = temperature;
    }

    @Override
    public int compareTo(YearTempPair o) {
        // 按年份升序、按气温值降序排序
        int compareFirst = Integer.compare(year, o.getYear());
        if (compareFirst != 0) {
            return compareFirst;
        } else {
            return (-1) * Integer.compare(temperature, o.getTemperature());
        }
    }

    @Override
    public int hashCode() {
        return year * 163 + temperature;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(year);
        dataOutput.writeInt(temperature);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readInt();
        temperature = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.year + " " + this.temperature;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }
}
