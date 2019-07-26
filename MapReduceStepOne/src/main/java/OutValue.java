import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OutValue implements WritableComparable<OutValue> {
    private LongWritable mBiGramOccurrences;
    private LongWritable mFirstWordOccurrences;
    private LongWritable mSecondWordOccurrences;
    private LongWritable mDecadeOccurrences;

    public OutValue(){
        mBiGramOccurrences     = new LongWritable(0);
        mFirstWordOccurrences  = new LongWritable(0);
        mSecondWordOccurrences = new LongWritable(0);
        mDecadeOccurrences     = new LongWritable(0);
    }

    public OutValue(LongWritable biGramOcc, LongWritable firstOcc, LongWritable secOcc, LongWritable decOcc){
        mBiGramOccurrences     = new LongWritable(biGramOcc.get());
        mFirstWordOccurrences  = new LongWritable(firstOcc.get());
        mSecondWordOccurrences = new LongWritable(secOcc.get());
        mDecadeOccurrences     = new LongWritable(decOcc.get());
    }


    public int compareTo(OutValue o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        mBiGramOccurrences.write(dataOutput);
        mFirstWordOccurrences.write(dataOutput);
        mSecondWordOccurrences.write(dataOutput);
        mDecadeOccurrences.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        mBiGramOccurrences.readFields(dataInput);
        mFirstWordOccurrences.readFields(dataInput);
        mSecondWordOccurrences.readFields(dataInput);
        mDecadeOccurrences.readFields(dataInput);
    }

    public LongWritable getBiGramOccurrences() {
        return mBiGramOccurrences;
    }

    public LongWritable getFirstWordOccurrences() {
        return mFirstWordOccurrences;
    }

    public LongWritable getSecondWordOccurrences() {
        return mSecondWordOccurrences;
    }

    public LongWritable getDecadeOccurrences() {
        return mDecadeOccurrences;
    }

    public String toString() {
        return mBiGramOccurrences.toString() + " " +
                mFirstWordOccurrences.toString() + " " +
                mSecondWordOccurrences.toString() + " " +
                mDecadeOccurrences.toString();
    }
}

