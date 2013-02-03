import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class XmlInputFormat extends TextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit is,
			TaskAttemptContext context) {
		return new XmlRecordReader();
	}

	public static class XmlRecordReader extends
			RecordReader<LongWritable, Text> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		@Override
		public void initialize(InputSplit is, TaskAttemptContext context)
				throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) is;
			startTag = context.getConfiguration().get("xmlStart")
					.getBytes("utf-8");
			endTag = context.getConfiguration().get("xmlEnd").getBytes("utf-8");
			start = fileSplit.getStart();
			end = start + fileSplit.getLength();
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(context.getConfiguration());
			fsin = fs.open(fileSplit.getPath());
			fsin.seek(start);
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if (fsin.getPos() < end) {
				if (readUntilMatch(startTag, false)) {
					try {
						buffer.write(startTag);
						if (readUntilMatch(endTag, true)) {
							value.set(buffer.getData(), 0, buffer.getLength());
							key.set(fsin.getPos());
							return true;
						}
					} finally {
						buffer.reset();
					}
				}
			}
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return (fsin.getPos() - start) / (float) (end - start);
		}

		@Override
		public void close() throws IOException {
			fsin.close();
		}

		private boolean readUntilMatch(byte[] match, boolean xmlStart)
				throws IOException {
			int i = 0;
			while (true) {
				int b = fsin.read();
				if (b < 0)
					return false;
				if (xmlStart)
					buffer.write(b);
				if (b == match[i]) {
					i++;
					if (i >= match.length)
						return true;
				} else
					i = 0;

				if (!xmlStart && i == 0 && fsin.getPos() >= end)
					return false;
			}
		}

	}

}