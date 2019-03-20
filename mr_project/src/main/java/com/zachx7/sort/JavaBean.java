package com.zachx7.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author zach - 吸柒
 */
public class JavaBean implements WritableComparable<JavaBean>{

    private String first;
    private Integer second;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return first + "\t" + second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       dataOutput.writeUTF(first);
       dataOutput.writeInt(second);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       this.first=dataInput.readUTF();
       this.second=dataInput.readInt();
    }

    @Override
    public int compareTo(JavaBean o) {

        int i = this.first.compareTo(o.getFirst());
        if(i != 0){
            return i;
        }else{
            int i1 = this.second.compareTo(o.getSecond());
            return i1;
        }
    }
}
