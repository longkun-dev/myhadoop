package im.yuki.myhadoop.ch6.entity;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/18 11:58 PM
 * @description 气温纪录实体类
 */
public class TempRecord {

    /**
     * 年份
     */
    private String year;

    /**
     * 当年度最大气温
     */
    private int temp;


    public void parse(String record) {
        year = record.substring(0, 4);
        temp = Integer.parseInt(record.substring(8, 10));
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "TempRecord{" +
                "year='" + year + '\'' +
                ", temp=" + temp +
                '}';
    }
}
