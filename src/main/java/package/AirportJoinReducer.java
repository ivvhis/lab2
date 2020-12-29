import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class AirportJoinReducer extends Reducer<KeyValueWritableComparable , Text, Text ,Text> {
    @Override
    protected void reduce(KeyValueWritableComparable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text airport = new Text(iter.next());
        if (iter.hasNext()) {
            Text airraceDelay = iter.next();
            int cnt = 1;
            double delay = Double.parseDouble(String.valueOf(airraceDelay));
            double maxDelay = Double.parseDouble(String.valueOf(airraceDelay));
            double minDelay = Double.parseDouble(String.valueOf(airraceDelay));
            while (iter.hasNext()) {
                airraceDelay = iter.next();
                double tmpNewDelay = Double.parseDouble(String.valueOf(airraceDelay));
                delay += tmpNewDelay;
                if (maxDelay < tmpNewDelay)
                    maxDelay = tmpNewDelay;
                if (minDelay > tmpNewDelay)
                    minDelay = tmpNewDelay;
                cnt++;
            }
            context.write(airport,
                    new Text("Min dealy : " + minDelay + " Max delay : " + maxDelay
                            + " Delay : " + delay / cnt));
        }
    }
}
