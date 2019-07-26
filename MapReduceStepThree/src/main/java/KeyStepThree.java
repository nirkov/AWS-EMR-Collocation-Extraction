
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyStepThree implements WritableComparable<KeyStepThree> {
    private Text        mFirstWord;
    private Text        mSecondWord;
    private IntWritable mDecade;
    public  Text        mType;

    /** @_WE_MUST_DEFAULT_CONSTRUCTOR_WHEN_IT_CLASS_USES_BY_HADOOP */
    public KeyStepThree(){
        mFirstWord  = new Text();
        mSecondWord = new Text();
        mDecade     = new IntWritable();
        mType       = new Text();
    }

    public KeyStepThree(Text lWord, Text rWord, IntWritable decade, Text newType){
        mFirstWord  = lWord;
        mSecondWord = rWord;
        mDecade     = decade;
        mType       = newType;
    }

    public Text getFirstWord() {return mFirstWord ;}

    public Text getSecondWord() {return mSecondWord ;}

    public IntWritable getDecade() {return mDecade ;}

    public int compareTo(KeyStepThree other) {
        final int    oDecade = other.getDecade().get();
        final String oFirst  = other.getFirstWord().toString();
        final String oType   = other.mType.toString();

        final String myType   = mType.toString();
        final String mySecond = mSecondWord.toString();
        final int myDecade    = mDecade.get();
        int ans;

        if(myDecade != oDecade){
            ans = (int) Math.signum(myDecade - oDecade);
        }else{ // Decades are equals
            if(myType.equals(oType)){
                ans = mySecond.compareTo(oFirst);
            }else{ // different type
                if(myType.equals("PMI")) { // Other is NGRAM type
                    ans = -1;
                }else{
                    ans = 1;
                }
            }
        }
        return (int)Math.signum(ans) ;
    }

    public void write(DataOutput out) throws IOException {
        mFirstWord.write(out);
        mSecondWord.write(out);
        mDecade.write(out);
        mType.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        mFirstWord.readFields(in);
        mSecondWord.readFields(in);
        mDecade.readFields(in);
        mType.readFields(in);
    }

    public String toString(){
        return mFirstWord.toString() + " " +
                mSecondWord.toString() + " " +
                mDecade.toString();
    }
}
