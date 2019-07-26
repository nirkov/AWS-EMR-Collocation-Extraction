
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyStepTwo implements WritableComparable<KeyStepTwo> {
    private Text        mFirstWord;
    private Text        mSecondWord;
    private IntWritable mDecade;
    public  Text        mType;

    /** @_WE_MUST_DEFAULT_CONSTRUCTOR_WHEN_IT_CLASS_USES_BY_HADOOP */
    public KeyStepTwo(){
        mFirstWord  = new Text();
        mSecondWord = new Text();
        mDecade     = new IntWritable();
        mType       = new Text();
    }

    public KeyStepTwo(Text lWord, Text rWord, IntWritable decade, Text newType){
        mFirstWord  = lWord;
        mSecondWord = rWord;
        mDecade     = decade;
        mType       = newType;
    }

    public Text getFirstWord() {return mFirstWord ;}

    public Text getSecondWord() {return mSecondWord ;}

    public IntWritable getDecade() {return mDecade ;}

    public int compareTo(KeyStepTwo other) {
        final int    oDecade = other.getDecade().get();
        final String oSecond = other.getSecondWord().toString();
        final String oType   = other.mType.toString();

        final int    myDecade = mDecade.get();
        final String myType   = mType.toString();
        final String mySecond = mSecondWord.toString();

        int ans = 0;

        if(myDecade != oDecade){
            ans = (int) Math.signum(myDecade - oDecade);
        }else{ // Decades are equals
            if(myType.equals(oType)){
                ans = mySecond.compareTo(oSecond);
            }else{ // different type
                if(myType.equals("SECOND")){ // Other is NGRAM type
                    if(mySecond.equals(oSecond)){
                        ans = -1;
                    }else{
                        ans = mySecond.compareTo(oSecond);
                    }
                }else { // my type is NGRAM other is Second
                    if(mySecond.equals(oSecond)){
                        ans = 1;
                    }else{
                        ans = mySecond.compareTo(oSecond);
                    }
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
