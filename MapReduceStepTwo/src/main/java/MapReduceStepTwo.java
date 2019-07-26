import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MapReduceStepTwo {
    public static class CE_Mapper extends Mapper<KeyStepTwo, OutValue, KeyStepTwo, OutValue> {

        public void map(KeyStepTwo key, OutValue value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class CE_Reducer extends Reducer<KeyStepTwo, OutValue, KeyStepTwo, DoubleWritable> {
        private final Text star = new Text("*");
        private long currentSecondWordOccurrences = 0;

        public void reduce(KeyStepTwo key, Iterable<OutValue> values, Context context) throws IOException, InterruptedException {
            String type    = key.mType.toString();
            OutValue value = values.iterator().next();

            if(type.equals("SECOND")){
                currentSecondWordOccurrences = value.getSecondWordOccurrences().get();
            }else if(type.equals("NGRAM")){
                double npmi = calcNpmi(value.getBiGramOccurrences().get(), value.getFirstWordOccurrences().get(),
                        currentSecondWordOccurrences, value.getDecadeOccurrences().get());

                context.write(key, new DoubleWritable(npmi));
                context.write(new KeyStepTwo(star, star, key.getDecade(), new Text("PMI")), new DoubleWritable(npmi));
            } else { //TODO for test in case of error
                context.write(key,  new DoubleWritable(0));
            }
        }

        private double calcNpmi(long Cw1w2, long Cw1, long Cw2, long N){
            double pmi = Math.log(Cw1w2) + Math.log(N) - Math.log(Cw1) - Math.log(Cw2);
            double denominator = Cw1w2/N;
            if(denominator == 1.0) denominator = 0.99;
            if(denominator == 0.0) denominator = 0.01;
            return pmi/(-1 * Math.log(denominator));
        }
    }


    public static class CE_Partitioner extends Partitioner<KeyStepTwo, OutValue> {

        public int getPartition(KeyStepTwo key, OutValue value, int numPartitions) {
            return (key.getDecade().get() % 100 /10) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        try{

            System.out.println("*******************************************************");
            System.out.println("                    START STEP TWO");
            System.out.println("*******************************************************");

            String inputePath  = args[0];
            String outputePath = args[1];

            System.out.println("Input path  : " + inputePath);
            System.out.println("Output path : " + outputePath);

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "Step Two");
            job.setJarByClass(MapReduceStepTwo.class);

            // Override of MapReduce parts - Mapper, Combiner, Partitioner and Reducer.
            job.setMapperClass(CE_Mapper.class);
            job.setReducerClass(CE_Reducer.class);
            job.setPartitionerClass(CE_Partitioner.class);

            // Map out is a LongWritable which is the count of all Appearance of KeyStepTwo.
            job.setMapOutputValueClass(OutValue.class);
            job.setMapOutputKeyClass(KeyStepTwo.class);

            //Output value is a StepOneOutValue object.
            job.setOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(KeyStepTwo.class);

            job.setInputFormatClass(InputFormatStepTwo.class);

            FileInputFormat.addInputPath(job, new Path(inputePath));
            FileOutputFormat.setOutputPath(job, new Path(outputePath));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(Exception e){
            System.out.println(e.getMessage());
            System.out.println(e.getCause());
        }
    }
}
