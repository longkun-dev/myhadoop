package im.yuki.myhadoop.ch9.entity;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/7 7:59 PM
 * @description 自定义键值对
 */
public class NationSpecialtyPair implements WritableComparable<NationSpecialtyPair> {

    private String code;

    private int order;

    public NationSpecialtyPair(String code, int order) {
        super();
        this.code = code;
        this.order = order;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(code);
        dataOutput.writeInt(order);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.code = dataInput.readUTF();
        this.order = dataInput.readInt();
    }

    @Override
    public String toString() {
        return this.code + " " + this.order;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    @Override
    public int compareTo(NationSpecialtyPair o) {
        int compare1 = this.code.compareTo(o.getCode());
        if (compare1 != 0) {
            return compare1;
        } else {
            return Integer.compare(this.order, o.getOrder());
        }
    }

    @Override
    public int hashCode() {
        return this.code.hashCode() * 17 + this.order;
    }
}
