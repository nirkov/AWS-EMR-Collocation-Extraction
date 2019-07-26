import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;

import java.io.IOException;



public class RecordReaderStepTwo extends RecordReader<KeyStepTwo, OutValue> {

    KeyStepTwo       mKey;
    OutValue         mValue;
    LineRecordReader mLineReader;

    public RecordReaderStepTwo() {
        mLineReader = new LineRecordReader();
    }

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        mLineReader.initialize(inputSplit, taskAttemptContext);
    }

    public boolean nextKeyValue() throws IOException {
        if (!mLineReader.nextKeyValue()) {
            return false;
        }
        try{
            String[] data = mLineReader.getCurrentValue().toString().split(" |\\\t");
            String   type = data[0].equals("*") ? "SECOND" : "NGRAM";
            mKey   = new KeyStepTwo(new Text(data[0]), new Text(data[1]), new IntWritable(Integer.parseInt(data[2])), new Text(type));
            mValue = new OutValue(new LongWritable(Long.parseLong(data[3])),
                    new LongWritable(Long.parseLong(data[4])),
                    new LongWritable(Long.parseLong(data[5])),
                    new LongWritable(Long.parseLong(data[6])));
            return true;
        }catch(Exception e){
            System.out.println(e.getMessage());
            return false;
        }
    }

    public KeyStepTwo getCurrentKey() throws IOException, InterruptedException {
        return mKey;
    }

    public OutValue getCurrentValue() throws IOException, InterruptedException {
        return mValue;
    }

    public float getProgress() throws IOException, InterruptedException {
        return mLineReader.getProgress();
    }

    public void close() throws IOException {
        mLineReader.close();
    }
}
