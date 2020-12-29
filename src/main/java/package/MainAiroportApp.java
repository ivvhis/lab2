import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.Path;

public class MainAiroportApp {
    public static void main(String[] args) throws Exception{
        if (args.length < 3){
            System.err.println("MainAiroportApp exception");
            System.exit(1);
        }
        Job job = Job.getInstance();
        job.setJarByClass(MainAiroportApp.class);
        job.setJobName("JoinJob sort");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AirraceJoinMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportJoinMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setPartitionerClass(AirportJoinPartitioner.class);
        job.setGroupingComparatorClass(GroupingAirportComparatorClass.class);
        job.setReducerClass(AirportJoinReducer.class);
        job.setMapOutputKeyClass(KeyValueWritableComparable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
