package contentionAware;
import contentionFree.Allocation;
import setting.Edge;
import setting.Task;
import setting.VM;

import java.util.Objects;

/*
 * Channel Allocation Information
 * @author zhang

 */
public class CAllocation extends Allocation{
    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public VM getSourceVM() {
        return sourceVM;
    }

    public void setSourceVM(VM sourceVM) {
        this.sourceVM = sourceVM;
    }

    public VM getDestVM() {
        return destVM;
    }

    public void setDestVM(VM destVM) {
        this.destVM = destVM;
    }

    public Edge getEdge() {
        return edge;
    }

    public void setEdge(Edge edge) {
        this.edge = edge;
    }

    public double getEdgeStartTime() {
        return edgeStartTime;
    }

    public void setEdgeStartTime(double edgeStartTime) {
        this.edgeStartTime = edgeStartTime;
    }

    public double getEdgeFinishTime() {
        return edgeFinishTime;
    }

    public void setEdgeFinishTime(double edgeFinishTime) {
        this.edgeFinishTime = edgeFinishTime;
    }

    public double getChannelUploadStartTime() {
        return channelUploadStartTime;
    }

    public void setChannelUploadStartTime(double channelUploadStartTime) {
        this.channelUploadStartTime = channelUploadStartTime;
    }

    public double getChannelUploadFinishTime() {
        return channelUploadFinishTime;
    }

    public void setChannelUploadFinishTime(double channelUploadFinishTime) {
        this.channelUploadFinishTime = channelUploadFinishTime;
    }

    public double getChannelDownloadStartTime() {
        return channelDownloadStartTime;
    }

    public void setChannelDownloadStartTime(double channelDownloadStartTime) {
        this.channelDownloadStartTime = channelDownloadStartTime;
    }

    public double getChannelDownloadFinishTime() {
        return channelDownloadFinishTime;
    }

    public void setChannelDownloadFinishTime(double channelDownloadFinishTime) {
        this.channelDownloadFinishTime = channelDownloadFinishTime;
    }

    public Boolean getFlag() {
        return flag;
    }

    public void setFlag(Boolean flag) {
        this.flag = flag;
    }

    private Task task;
    private VM sourceVM;
    private VM destVM;
    private Edge edge;
    private double edgeStartTime;
    private double edgeFinishTime;
    private double channelUploadStartTime;
    private double channelUploadFinishTime;
    private double channelDownloadStartTime;
    private double channelDownloadFinishTime;
    private Boolean flag = false;

    protected CAllocation(){}
    public <S extends VM,D extends VM> CAllocation(Edge edge,S sourceVM,D destVM,  //私有云->公有云 flag = true;
                                      double edgeStartTime,double edgeFinishTime,double channelUploadStartTime
                                        ,double channelUploadFinishTime,double channelDownloadStartTime,
                                     double channelDownloadFinishTime,Boolean flag) {
        this.edge = edge;
        this.sourceVM= sourceVM;
        this.destVM = destVM;
        this.edgeStartTime = edgeStartTime;
        this.edgeFinishTime = edgeFinishTime;
        this.channelUploadStartTime = channelUploadStartTime;
        this.channelUploadFinishTime = channelUploadFinishTime;
        this.flag = flag;
        this.channelDownloadStartTime = channelDownloadStartTime;
        this.channelDownloadFinishTime = channelDownloadFinishTime;
    }


    //adapter，判断是否是同一条边
    public Boolean containsEAllocation(EAllocation eAllocation){
        if(eAllocation.getEdge() == this.getEdge()){
            return true;
        }
        else{
            return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null ) return false;
        CAllocation that = (CAllocation) o;
        return Double.compare(that.edgeStartTime, edgeStartTime) == 0 && Double.compare(that.edgeFinishTime, edgeFinishTime) == 0
                && sourceVM.getAttribute().equals(that.getSourceVM().getAttribute())
                &&sourceVM.getId().equals(that.getSourceVM().getId())
                &&destVM.getId().equals(that.getDestVM().getId())
                &&destVM.getAttribute().equals(that.getDestVM().getAttribute())
                && Objects.equals(edge, that.edge) && Objects.equals(flag, that.flag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(task, sourceVM, destVM, edge, edgeStartTime, edgeFinishTime, flag);
    }

    @Override
    public String toString() {
        return "CAllocation{" +
                ", edge=" + edge +
                ", sourceVM=" + sourceVM +
                ", destVM=" + destVM +
                ", edgeStartTime=" + edgeStartTime +
                ", edgeFinishTime=" + edgeFinishTime +
                ", flag=" + flag +
                '}';
    }
}
