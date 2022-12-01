package setting;

import java.util.Objects;

/**
 * an edge in the workflow
 */
public class Edge {

    private Task source;
    private Task destination;
    private double dataSize;

    /**
     * constructs an edge with two tasks: @param source, @param destination
     */
    public Edge(Task source,Task destination){
        this.source = source;
        this.destination = destination;
    }
    /**
    ----getters && setters-----
    */
    //获取公有云边的传输时间
    public double getPublicCloudTransferTime(){
        return this.dataSize/VM_Public.NETWORK_SPEED;
    }

    //获取私有云边的传输时间
    public double getPrivateCloudTransferTime(){
        return this.dataSize/VM_Private.NETWORK_SPEED;
    }


    //获取当前边的源节点
    public Task getSource() {
        return this.source;
    }

    //获取当前边的目标节点
    public Task getDestination(){return this.destination;}

    //获取当前数据大小
    public double getDataSize() {return this.dataSize;}

    //only used by WorkflowParser class
    //只在工作流解析时用到
    public void setDataSize(double size) {this.dataSize = size;}

    @Override
    public String toString() {
        return "Edge{" +
                "source=" + source.getName() +
                ", destination=" + destination.getName() +
                ", dataSize=" + dataSize +
                '}';
    }




}
