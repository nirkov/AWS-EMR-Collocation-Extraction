import java.io.IOException;

import com.amazonaws.regions.Regions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;


import java.util.Date;

public class MapReduceStepOne {

    public static class CE_Mapper extends Mapper<LongWritable, Text, KeyStepOne, LongWritable> {
        private final Text star = new Text("*");
        /**
         * @map
         * @param value (Google Books Ngrams) is a tab separated string containing the following fields:
         *          1. n-gram      - The actual n-gram
         *          2. year        - The year for this aggregation
         *          3. occurrences - The number of times this n-gram appeared in this year
         *          4. pages       - The number of pages this n-gram appeared on in this year
         *          5. books       - The number of books this n-gram appeared in during this year
         *
         *        **https://aws.amazon.com/datasets/google-books-ngrams/**
         *        **https://archive.org/details/google_ngrams-eng-all-2grams**
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Split the data in this row/
            String dataRow [] = value.toString().split("\t");

            // Take the words in ngram and check if this is 2-gram.
            String[] nGram   = dataRow[0].split(" ");
            if(nGram.length != 2) return;

            Text lWord  = new Text(nGram[0].replaceAll("[^A-Za-z0-9]", "").toLowerCase());
            Text rWord  = new Text(nGram[1].replaceAll("[^A-Za-z0-9]", "").toLowerCase());

            IntWritable  decade      = new IntWritable();
            LongWritable occurrences = new LongWritable();

            try{
                int year = Integer.parseInt(dataRow[1]);

                // Extract decade --> decade(1981) = 1980.
                decade.set(year - (year % 10));
                occurrences.set(Long.parseLong(dataRow[2]));

            }catch(NumberFormatException e){
                System.out.println(e.getCause());
                return;
            }

            // The this case of map reduce, {key, value} can be -
            // 1. Key = {lWord, rWord, decade} and Value = occurrences
            context.write(new KeyStepOne(lWord, rWord, decade, new Text("NGRAM")), occurrences);

            // 2. Key = {lWord, * , decade} and Value = occurrences (occurrences of lWord)
            context.write(new KeyStepOne(lWord, star, decade, new Text("FIRST")), occurrences);

            // 3. Key = { * ,rWord, decade} and Value = occurrences (occurrences of rWord)
            context.write(new KeyStepOne(star , rWord, decade, new Text("SECOND")), occurrences);

            // 4. Key = { * , * , decade} and Value = occurrences (occurrences of decade which is |Corpus|)
            context.write(new KeyStepOne(star , star, decade, new Text("DECADE")), occurrences);
        }
    }

    public static class CE_Reducer extends Reducer<KeyStepOne, LongWritable, KeyStepOne, OutValue> {
        private long currentFirstWordOccurrences = 0;
        private long currentDecadeOccurrences    = 0;

        public void reduce(KeyStepOne key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;

            for (LongWritable value : values) {
                count += value.get();
            }

            String type = key.mType.toString();
            if(type.equals("DECADE")){
                currentDecadeOccurrences = count;
            }else if(type.equals("FIRST")){
                currentFirstWordOccurrences = count;
            }else if(type.equals("NGRAM")){
                OutValue outValueNgram = new OutValue(new LongWritable(count),
                        new LongWritable(currentFirstWordOccurrences),
                        new LongWritable(0),
                        new LongWritable(currentDecadeOccurrences));
                context.write(key, outValueNgram);
            }else{
                OutValue outValueSecond = new OutValue(new LongWritable(0),
                        new LongWritable(0),
                        new LongWritable(count),
                        new LongWritable(currentDecadeOccurrences));
                context.write(key, outValueSecond);
            }
        }
    }


    public static class CE_Combiner extends Reducer<KeyStepOne, LongWritable, KeyStepOne, LongWritable> {

        @Override
        public void reduce(KeyStepOne key, Iterable<LongWritable> values, Context context) throws IOException,  InterruptedException {
            long totalOccurrences = 0;
            for (LongWritable value : values) {
                totalOccurrences += value.get();
            }
            context.write(key, new LongWritable(totalOccurrences));
        }
    }

    public static class CE_Partitioner extends Partitioner<KeyStepOne, LongWritable> {

        public int getPartition(KeyStepOne key, LongWritable value, int numPartitions) {
            return (key.getDecade().get() % 100 /10) % numPartitions;
        }
    }


    public static void main(String[] args) throws Exception {
        try{
            System.out.println("*******************************************************");
            System.out.println("                    START STEP ONE");
            System.out.println("*******************************************************");

            String inputePath  = args[0];
            String outputePath = args[1] ;

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "Step One");
            job.setJarByClass(MapReduceStepOne.class);

            // Override of MapReduce parts - Mapper, Combiner, Partitioner and Reducer.
            job.setMapperClass(CE_Mapper.class);
            job.setCombinerClass(CE_Combiner.class);
            job.setReducerClass(CE_Reducer.class);
            job.setPartitionerClass(CE_Partitioner.class);

            // Map out is a LongWritable which is the count of all Appearance of nGramKey.
            job.setMapOutputValueClass(LongWritable.class);
            job.setMapOutputKeyClass(KeyStepOne.class);

            //Output value is a OutValue object.
            job.setOutputValueClass(OutValue.class);
            job.setOutputKeyClass(KeyStepOne.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);

            FileInputFormat.addInputPath(job, new Path(inputePath));
            FileOutputFormat.setOutputPath(job, new Path(outputePath));
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        }catch(Exception e){
            System.out.println(e.getMessage());
            System.out.println(e.getCause());
        }
    }
}

