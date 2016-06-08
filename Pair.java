import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements Writable, WritableComparable<Pair> {
	Text  Left;
	Text  Right;

    public Pair() {
    	this.Left = new Text();
        this.Right = new Text();
    }

    public Pair(String left, String right) {
    	this.Left = new Text(left);
        this.Right = new Text(right);
    }

    public Text getLeft() {
        return Left;
    }

    @Override
	public String toString() {
		return "Pair (Left=" + Left + ", Right=" + Right + ")";
	}

	public Text getRight() {
        return Right;
    }

    @Override
	public int hashCode() {
		int result=17* Left.hashCode() +19*Right.hashCode();
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
		
		if(other.getLeft().equals(Left)&&other.getRight().equals(Right))
			return true;
		else
			return false;
		
	}

	@Override
    public int compareTo(Pair pair) {
		/*
    	if(this.Right.toString().equals("*"))
    		return 1;
        if (this.Left.compareTo(pair.Left) > 0)
            return 1;
        else if (this.Left.compareTo(pair.Left) < 0)
            return -1;
        else
            return this.Right.compareTo(pair.Right);
        */
		if (this.Left.compareTo(pair.Left) > 0) return 1;
        else if (this.Left.compareTo(pair.Left) < 0) return -1;
		if(this.Right.toString().equals("*")) return -1;
		return this.Right.compareTo(pair.Right);
		
    }

	 @Override
	    public void write(DataOutput out) throws IOException {
	        Left.write(out);
	        Right.write(out);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	        Left.readFields(in);
	        Right.readFields(in);
	    }

	    public static Pair read(DataInput in) throws IOException {
	        Pair w = new Pair();
	        w.readFields(in);
	        return w;
	    }
}
