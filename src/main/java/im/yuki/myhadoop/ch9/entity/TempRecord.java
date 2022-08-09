package im.yuki.myhadoop.ch9.entity;

/**
 * @author longkun
 * @version V1.0
 * @date 2022/8/6 6:58 PM
 * @description 气温记录实体类
 */
public class TempRecord {

    private String stationId;

    private String date;

    private int temperature;

    public TempRecord() {
    }

    public void parse(String record) {
        this.stationId = record.substring(0, 2);
        this.date = record.substring(2, 10);
        this.temperature = Integer.parseInt(record.substring(10, 12));
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public int getTemperature() {
        return temperature;
    }

    public void setTemperature(int temperature) {
        this.temperature = temperature;
    }
}
