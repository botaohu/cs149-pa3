import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextIntPair implements WritableComparable<TextIntPair> {

	public Text first;
	public IntWritable second;

	public Text getFirst() {
		return first;
	}

	public IntWritable getSecond() {
		return second;
	}

	public TextIntPair() {

		first = new Text();
		second = new IntWritable();
	}

	public TextIntPair(Text first, IntWritable second) {
		this.first = first;
		this.second = second;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);

	}

	@Override
	public int compareTo(TextIntPair that) {
		int cmp = -second.compareTo(that.second);
		if (cmp == 0) {
			return -first.compareTo(that.first);
		}
		return cmp;
	}

	@Override
	public boolean equals(Object obj) {

		if (obj instanceof TextIntPair) {
			TextIntPair that = (TextIntPair) obj;
			return (first.equals(that.first) && second.equals(that.second));
		}

		return false;
	}

	@Override
	public int hashCode() {
		return first.hashCode() + 167 * second.hashCode();
	}

	@Override
	public String toString() {
		return first.toString() + "-" + second.toString();
	}
}
