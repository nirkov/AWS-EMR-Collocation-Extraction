import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class InputFormatStepTwo extends FileInputFormat<KeyStepTwo, OutValue> {

    public RecordReader<KeyStepTwo, OutValue> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new RecordReaderStepTwo();
    }

    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}
