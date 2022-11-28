package contentionAware;

import contentionFree.Allocation;
import contentionFree.TAllocation;
import setting.Edge;
import setting.VM;

import java.util.Objects;

/**
 * Edge Allocation Information
 * @author wu
 */
public class EAllocation extends Allocation{
	private Edge edge;
	private VM sourceVM, destVM;

//	不需要存储source & destination task allocations，因为只要有task和vm就能找到相应的TaskAllocation
	
	public EAllocation(Edge edge, VM sourceVM, VM destVM, double startTime,double speed){
		this.edge = edge;
		this.sourceVM = sourceVM;
		this.destVM = destVM;
		this.startTime = startTime;
		this.finishTime = startTime + edge.getDataSize() / speed ;
	}
	
	public EAllocation(Edge edge, VM sourceVM, VM destVM, double startTime,double speed, double finishTime){
		this(edge, sourceVM, destVM, startTime,speed);
		this.finishTime = finishTime ;
	}

	public EAllocation(Edge edge, VM sourceVM, VM destVM, double startTime){
		this.edge = edge;
		this.sourceVM = sourceVM;
		this.destVM = destVM;
		this.startTime = startTime;
		this.finishTime = startTime + edge.getDataSize() / VM.NETWORK_SPEED ;
	}
	
	//used by Adaptor的waitingEdges，需要setStartTime
	public EAllocation(Edge edge, VM sourceVM, VM destVM){
		this.edge = edge;
		this.sourceVM = sourceVM;
		this.destVM = destVM;
	}
	//used by Adaptor
	public void setStartTime(double startTime) {
		this.startTime = startTime;
		//this.finishTime = startTime + edge.getDataSize() / VM.NETWORK_SPEED ;
		this.finishTime = startTime + edge.getDataSize();
	}
	
	//----------------------------getters-----------------------------------
	public Edge getEdge() {		return edge;	}
	public VM getSourceVM() {	return sourceVM;		}
	public VM getDestVM() {	return destVM;		}
	
	//----------------------------override-----------------------------------
    public boolean equals(Object o) {

		if (this == o) return true;
		EAllocation that = (EAllocation) o;
		return sourceVM.getAttribute().equals(that.getSourceVM().getAttribute())
				&&sourceVM.getId().equals(that.getSourceVM().getId())
				&&destVM.getId().equals(that.getDestVM().getId())
				&&destVM.getAttribute().equals(that.getDestVM().getAttribute())
				&&Math.abs(startTime- that.startTime)<=0.0001
				&&Math.abs(finishTime- that.finishTime)<=0.0001;
    }
	public String toString() {
		return "EAllocation [edge = " + edge.getSource().getName()+ " -> " 
				+ edge.getDestination().getName()
				+ ": " + startTime + ", " + finishTime + "]";
	}
}