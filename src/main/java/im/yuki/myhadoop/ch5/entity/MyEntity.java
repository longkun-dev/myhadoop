package im.yuki.myhadoop.ch5.entity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/20 4:02 PM
 * @description 定制的 Writable 集合
 */
public class MyEntity implements WritableComparable<MyEntity> {

    private Text code;

    private Text name;

    public MyEntity(String code, String name) {
        this.code = new Text(code);
        this.name = new Text(name);
    }

    public MyEntity(Text code, Text name) {
        this.code = code;
        this.name = name;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        code.write(dataOutput);
        name.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        code.readFields(dataInput);
        name.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return this.code.hashCode() * 17 + this.name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MyEntity) {
            MyEntity entity = (MyEntity) obj;
            return this.code.equals(entity.getCode()) && this.name.equals(entity.getCode());
        }
        return false;
    }

    @Override
    public String toString() {
        return "MyEntity: [code=" + this.code.toString() + ", name=" + this.name.toString() + "]";
    }

    @Override
    public int compareTo(MyEntity o) {
        int c1 = this.code.compareTo(o.getCode());
        if (c1 != 0) {
            return c1;
        }
        return this.name.compareTo(o.getName());
    }

    public Text getCode() {
        return code;
    }

    public void setCode(Text code) {
        this.code = code;
    }

    public Text getName() {
        return name;
    }

    public void setName(Text name) {
        this.name = name;
    }
}
