import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class InputFormatStepThree extends FileInputFormat<KeyStepThree, DoubleWritable> {

    public RecordReader<KeyStepThree, DoubleWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReaderStepThree();
    }

    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
