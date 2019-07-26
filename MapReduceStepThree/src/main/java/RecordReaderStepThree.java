import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.io.Text;

import java.io.IOException;



public class RecordReaderStepThree extends RecordReader<KeyStepThree, DoubleWritable> {

    KeyStepThree     mKey;
    DoubleWritable   mValue;
    LineRecordReader mLineReader;
    private final Text star = new Text("*");

    public RecordReaderStepThree() {
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
            String   type = data[0].equals("*") ? "PMI" : "NGRAM";
            if(type.equals("PMI")){
                mKey = new KeyStepThree(star, star, new IntWritable(Integer.parseInt(data[2])), new Text(type));
            }else{
                mKey = new KeyStepThree(new Text(data[0]), new Text(data[1]), new IntWritable(Integer.parseInt(data[2])), new Text(type));
            }
            mValue = new DoubleWritable(Double.parseDouble(data[3]));
            return true;
        }catch(Exception e){
            System.out.println(e.getMessage());
            return false;
        }
    }

    public KeyStepThree getCurrentKey() throws IOException, InterruptedException {
        return mKey;
    }

    public DoubleWritable getCurrentValue() throws IOException, InterruptedException {
        return mValue;
    }

    public float getProgress() throws IOException, InterruptedException {
        return mLineReader.getProgress();
    }

    public void close() throws IOException {
        mLineReader.close();
    }
}
