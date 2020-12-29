import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AirportJoinPartitioner extends Partitioner<KeyValueWritableComparable, Text> {
    @Override
    public int getPartition(KeyValueWritableComparable key, Text text, int numReduceTasks) {
        return (key.getAirportID() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
