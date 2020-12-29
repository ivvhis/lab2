import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.Key;
import java.util.Objects;

public class KeyValueWritableComparable implements WritableComparable<KeyValueWritableComparable> {
    private int airportPartitionerKey;
    private int airportID;
    public KeyValueWritableComparable(){
        airportPartitionerKey = 0 ;
        airportID = 0;
    }

    public void setAirportPartitionerKey(int airportPartitionerKey) {
        this.airportPartitionerKey = airportPartitionerKey;
    }

    public KeyValueWritableComparable(int airportID , int airportPartitionerKey) {
        this.airportPartitionerKey = airportPartitionerKey;
        this.airportID = airportID;
    }

    public int getAirportPartitionerKey() {
        return airportPartitionerKey;
    }
    public int getAirportID() {
        return airportID;
    }
    @Override
    public int compareTo(KeyValueWritableComparable o) {
        if (this.airportID > o.airportID)
            return 1;
        else if (this.airportID != o.airportID)
            return -1;
        if (this.airportPartitionerKey > o.airportPartitionerKey)
            return 1;
        else if (this.airportPartitionerKey != o.airportPartitionerKey)
            return -1;
        return 0;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(airportPartitionerKey);
        out.writeInt(airportID);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        airportPartitionerKey = in.readInt();
        airportID = in.readInt();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyValueWritableComparable that = (KeyValueWritableComparable) o;
        return airportPartitionerKey == that.airportPartitionerKey &&
                airportID == that.airportID;
    }

    @Override
    public String toString() {
        return "KeyValueWritableComparable{" +
                "airportPartitionerKey='" + airportPartitionerKey + '\'' +
                ", airportID='" + airportID + '\'' +
                '}';
    }
    public int compareAirportID(KeyValueWritableComparable o){
        if (this.airportID > o.airportID)
            return 1;
        else if (this.airportID != o.airportID)
            return -1;
        return 0;
    }
    public int compare(KeyValueWritableComparable a , KeyValueWritableComparable b){
        return a.compareTo(b);
    }
    @Override
    public int hashCode() {
        return Objects.hash(airportPartitionerKey, airportID);
    }
}
