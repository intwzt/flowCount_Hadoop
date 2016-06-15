package snnu.hadoop.flowsort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class flowBean implements WritableComparable<flowBean>{
	
	private String id;
	private Long up_flow;
	private Long down_flow;
	private Long sum_flow;
	
	public void set(String id, Long up_flow2, Long down_flow2){
		this.id  = id;
		this.up_flow = up_flow2;
		this.down_flow = down_flow2;
		this.sum_flow =  up_flow2 + down_flow2;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Long getUp_flow() {
		return up_flow;
	}

	public void setUp_flow(Long up_flow) {
		this.up_flow = up_flow;
	}

	public Long getDown_flow() {
		return down_flow;
	}

	public void setDown_flow(Long down_flow) {
		this.down_flow = down_flow;
	}

	public Long getSum_flow() {
		return sum_flow;
	}

	public void setSum_flow(Long sum_flow) {
		this.sum_flow = sum_flow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		
		id = in.readUTF();
		up_flow = in.readLong();
		down_flow = in.readLong();
		sum_flow = in.readLong();
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(id);
		out.writeLong(up_flow);
		out.writeLong(down_flow);
		out.writeLong(sum_flow);
	}

	@Override
	public int compareTo(flowBean o) {
		return (sum_flow > o.getSum_flow() ? -1 : 1);
	}
	
	@Override
	public String toString() {
		return up_flow + "\t" + down_flow + "\t" + sum_flow;
	}
}
