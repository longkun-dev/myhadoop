package im.yuki.myhadoop.ch6.entity;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/7/23 10:18 PM
 * @description 气温记录值
 */
public class TempRecord1 {

    /**
     * 气象站
     */
    private String station;

    /**
     * 测温日期
     */
    private String date;

    /**
     * 温度
     */
    private int temp;

    public void parse(String dataLine) {
        this.station = dataLine.substring(0, 2);
        this.date = dataLine.substring(3, 11);
        this.temp = Integer.parseInt(dataLine.substring(17));
    }

    public String getStation() {
        return station;
    }

    public void setStation(String station) {
        this.station = station;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    @Override
    public String toString() {
        return "TempRecord1{" +
                "station='" + station + '\'' +
                ", date='" + date + '\'' +
                ", temp=" + temp +
                '}';
    }
}
