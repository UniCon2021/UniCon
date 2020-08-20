package utils;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntPairWritable implements Writable {

    public int u, v;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(v);
        dataOutput.writeInt(u);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        v = dataInput.readInt();
        u = dataInput.readInt();
    }

    public void set(int u, int v) {
        this.u = u;
        this.v = v;
    }
}
