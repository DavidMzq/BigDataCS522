
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements Writable, WritableComparable<Pair> {
	Text  Key;
	Text  value;

    public Pair() {
    	this.Key = new Text();
        this.value = new Text();
    }

    public Pair(String key, String value) {

    	this.Key = new Text(key);
        this.value = new Text(value);
    }

    public Text getKey() {
        return Key;
    }

    @Override
	public String toString() {
		return "Pair [Key=" + Key + ", value=" + value + "]";
	}

	public Text getValue() {
        return value;
    }


    @Override
	public int hashCode() {
		int result=17* Key.hashCode() +19*value.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		
		if(other.getKey().equals(Key)&&other.getValue().equals(value))
			return true;
		else
			return false;
		
	}

	@Override
    public int compareTo(Pair pair) {
        // TODO Auto-generated method stub
    	if(this.value.equals("*"))
    		return -1;
        if (this.Key.compareTo(pair.Key) > 0)
            return 1;
        else if (this.Key.compareTo(pair.Key) < 0)
            return -1;
        else
            return this.value.compareTo(pair.value);
    }

	 @Override
	    public void write(DataOutput out) throws IOException {
	        // TODO Auto-generated method stub
	        Key.write(out);
	        value.write(out);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	        // TODO Auto-generated method stub
	        Key.readFields(in);
	        value.readFields(in);
	    }

	    public static Pair read(DataInput in) throws IOException {
	        Pair w = new Pair();
	        w.readFields(in);
	        return w;
	    }
}

