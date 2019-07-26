import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceStepThree {
    public static class CE_Mapper extends Mapper<KeyStepThree, DoubleWritable, KeyStepThree, DoubleWritable> {

        public void map(KeyStepThree key, DoubleWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class CE_Reducer extends Reducer<KeyStepThree, DoubleWritable, KeyStepThree, Text> {
        private double currentDecadeSumPMI = 0;

        public void reduce(KeyStepThree key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            if (key.mType.toString().equals("PMI")) {
                currentDecadeSumPMI = 0;
                for (DoubleWritable temp : values) {
                    currentDecadeSumPMI += temp.get();
                }
            } else {
                final double nPMI = values.iterator().next().get();
                final double rPMI = nPMI / currentDecadeSumPMI;
                final double minPmi = Double.parseDouble(context.getConfiguration().get("minPmi"));
                final double relMinPmi = Double.parseDouble(context.getConfiguration().get("relMinPmi"));

                if (nPMI >= minPmi || relMinPmi >= relMinPmi) {
                    context.write(key, new Text("relative min PMI: " + rPMI + " min PMI: " + nPMI));
                }
            }
        }
    }


    public static class CE_Partitioner extends Partitioner<KeyStepThree, DoubleWritable> {

        public int getPartition(KeyStepThree key, DoubleWritable value, int numPartitions) {
            return (key.getDecade().get() % 100 /10) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        try{

            System.out.println("*******************************************************");
            System.out.println("                    START STEP THREE");
            System.out.println("*******************************************************");

            String inputePath  = args[0];
            String outputePath = args[1];

            System.out.println("Input path  : " + inputePath);
            System.out.println("Output path : " + outputePath);

            Configuration conf = new Configuration();
            conf.set("minPmi", args[2]);
            conf.set("relMinPmi", args[3]);

            Job job = Job.getInstance(conf, "Step Three");
            job.setJarByClass(MapReduceStepThree.class);

            // Override of MapReduce parts - Mapper, Combiner, Partitioner and Reducer.
            job.setMapperClass(CE_Mapper.class);
            job.setReducerClass(CE_Reducer.class);
            job.setPartitionerClass(CE_Partitioner.class);

            // Map out is a LongWritable which is the count of all Appearance of KeyStepTwo.
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setMapOutputKeyClass(KeyStepThree.class);

            //Output value is a StepOneOutValue object.
            job.setOutputValueClass(Text.class);
            job.setOutputKeyClass(KeyStepThree.class);

            job.setInputFormatClass(InputFormatStepThree.class);

            FileInputFormat.addInputPath(job, new Path(inputePath));
            FileOutputFormat.setOutputPath(job, new Path(outputePath));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(Exception e){
            System.out.println(e.getMessage());
            System.out.println(e.getCause());
        }
    }
}
