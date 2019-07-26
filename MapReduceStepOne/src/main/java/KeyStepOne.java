import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KeyStepOne implements WritableComparable<KeyStepOne> {
    private Text        mFirstWord;
    private Text        mSecondWord;
    private IntWritable mDecade;
    public  Text        mType;

    /** @_WE_MUST_DEFAULT_CONSTRUCTOR_WHEN_IT_CLASS_USES_BY_HADOOP */
    public KeyStepOne(){
        mFirstWord  = new Text();
        mSecondWord = new Text();
        mDecade     = new IntWritable();
        mType       = new Text();
    }

    public KeyStepOne(Text lWord, Text rWord, IntWritable decade, Text newType){
        mFirstWord  = lWord;
        mSecondWord = rWord;
        mDecade     = decade;
        mType       = newType;
    }

    public Text getFirstWord() {return mFirstWord ;}

    public Text getSecondWord() {return mSecondWord ;}

    public IntWritable getDecade() {return mDecade ;}

    /**
     * @The_Notorious_comperTo
     * for z > x > y and dec1 > dec2
     * {*,*,dec1} > {x,*,dec1} > {x,z,dec1} > {x,y,dec1} > {y,*,dec1} > {y,z,dec1}
     * > {y,x,dec1} > {*,*,dec2} ...
     * @param other
     * @return
     */
    public int compareTo(KeyStepOne other) {
        final int    oDecade = other.getDecade().get();
        final String oFirst  = other.getFirstWord().toString();
        final String oSecond = other.getSecondWord().toString();
        final String oType   = other.mType.toString();

        final String myType = mType.toString();
        int ans = 0;

        if(mDecade.get() != oDecade ||
                (myType.equals("DECADE") & oType.equals("DECADE"))){
            return (int)Math.signum(mDecade.get() - oDecade);
        } else {
            if (myType.equals(oType)) { // Equals decades and both are not a DECADE but equal type.
                if(myType.equals("FIRST")){
                    ans = mFirstWord.toString().compareTo(oFirst);
                }else if(myType.equals("SECOND")){
                    ans =  mSecondWord.toString().compareTo(oSecond);
                }else {
                    if (mFirstWord.toString().equals(oFirst)) {
                        ans =  mSecondWord.toString().compareTo(oSecond);
                    } else ans = mFirstWord.toString().compareTo(oFirst);
                }
            } else { // Equals decades and different type.
                if (myType.equals("DECADE")){
                    ans = -1;
                }else if (oType.equals("DECADE")){
                    ans = 1;
                }else if (myType.equals("FIRST")){
                    if(oType.equals("SECOND")){
                        ans = -1;
                    }else if(oType.equals("NGRAM")){
                        if(mFirstWord.toString().equals(oFirst)){
                            ans = -1;
                        }else ans = mFirstWord.toString().compareTo(oFirst);
                    }
                }else if (oType.equals("FIRST")){
                    if(myType.equals("SECOND")){
                        ans =  1;
                    }else if(myType.equals("NGRAM")){
                        if(mFirstWord.toString().equals(oFirst)){
                            ans = 1;
                        }else ans = mFirstWord.toString().compareTo(oFirst);
                    }
                }else if (myType.equals("NGRAM")){
                    ans =  -1;
                }else ans = 1;
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
