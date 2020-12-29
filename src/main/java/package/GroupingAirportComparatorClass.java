import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingAirportComparatorClass extends WritableComparator {
    protected GroupingAirportComparatorClass() {
        super(KeyValueWritableComparable.class , true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        KeyValueWritableComparable airportFirstPair = (KeyValueWritableComparable) a;
        KeyValueWritableComparable airportSecondPair = (KeyValueWritableComparable) b;
        return airportFirstPair.compareAirportID(airportSecondPair);
    }
}