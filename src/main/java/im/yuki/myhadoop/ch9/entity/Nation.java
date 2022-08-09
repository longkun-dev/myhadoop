package im.yuki.myhadoop.ch9.entity;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/7 5:53 PM
 * @description 国家实体类
 */
public class Nation implements Writable, Comparable<Nation> {

    /**
     * 国家代码
     */
    private String nationCode;

    /**
     * 国家全称
     */
    private String nationName;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(nationCode);
        dataOutput.writeUTF(nationName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.nationCode = dataInput.readUTF();
        this.nationName = dataInput.readUTF();
    }

    @Override
    public int compareTo(Nation o) {
        return this.nationCode.compareTo(o.getNationCode());
    }

    public String getNationCode() {
        return nationCode;
    }

    public void setNationCode(String nationCode) {
        this.nationCode = nationCode;
    }

    public String getNationName() {
        return nationName;
    }

    public void setNationName(String nationName) {
        this.nationName = nationName;
    }

    @Override
    public String toString() {
        return "nationCode: " + nationCode + "  nationName: " + nationName;
    }
}
