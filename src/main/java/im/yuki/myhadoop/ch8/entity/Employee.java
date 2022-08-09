package im.yuki.myhadoop.ch8.entity;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/31 12:40 AM
 * @description 对应 MySQL 数据库
 */
public class Employee implements Writable, DBWritable {

    private String no;

    private String name;

    private String sex;

    private int age;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        Text.writeString(dataOutput, this.no);
        Text.writeString(dataOutput, this.name);
        Text.writeString(dataOutput, this.sex);
        dataOutput.writeInt(age);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.no = Text.readString(dataInput);
        this.name = Text.readString(dataInput);
        this.sex = Text.readString(dataInput);
        this.age = dataInput.readInt();
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1, this.no);
        preparedStatement.setString(2, this.name);
        preparedStatement.setString(3, this.sex);
        preparedStatement.setInt(4, this.age);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        resultSet.getString(1);
        resultSet.getString(2);
        resultSet.getString(3);
        resultSet.getInt(4);
    }

    public String getNo() {
        return no;
    }

    public void setNo(String no) {
        this.no = no;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}
