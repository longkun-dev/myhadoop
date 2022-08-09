package im.yuki.myhadoop.ch9.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/7 6:03 PM
 * @description 特产实体类
 */
public class Specialty implements Writable {

    /**
     * 特产名称
     */
    private String specialtyName;

    /**
     * 国家代码
     */
    private String nationCode;

    /**
     * 特产描述
     */
    private String specialtyDesc;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(specialtyName);
        dataOutput.writeUTF(nationCode);
        dataOutput.writeUTF(specialtyDesc);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.specialtyName = dataInput.readUTF();
        this.nationCode = dataInput.readUTF();
        this.specialtyDesc = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "name: " + specialtyName + " code: " + nationCode + " desc: " + specialtyDesc;
    }
}
