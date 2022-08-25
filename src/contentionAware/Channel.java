package contentionAware;

import setting.Edge;

import java.util.HashMap;
import java.util.Objects;

public class Channel {


    public Channel(String name){
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Channel channel = (Channel) o;
        return Objects.equals(name, channel.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    String name;
    //HashMap<StartTime,Edge>
    //用来记录每个经过通道的传输边的开始时间
    //为什么不用Edge当键，因为不方便按时间进行键排序,以时间为键自动第一个StartTime对应第一个Finshtime
    private static HashMap<Double, Edge> hashMapUploadStartTime ;
    private static HashMap<Double, Edge> hashMapUploadFinishTime ;
    private static HashMap<Double,Edge> hashMapDownloadStartTime ;
    private static HashMap<Double,Edge> hashMapDownloadFinishTime ;

    //跨云10m
    //private final static double transferSpeed = 10 * 1000;
    private final static double transferSpeed = 25;

    //---------------------getter and setter-----------
    public static HashMap<Double, Edge> getHashMapUploadStartTime() {
        return hashMapUploadStartTime;
    }

    public static void setHashMapUploadStartTime(HashMap<Double, Edge> hashMapUploadStartTime) {
        Channel.hashMapUploadStartTime = hashMapUploadStartTime;
    }

    public static HashMap<Double, Edge> getHashMapUploadFinishTime() {
        return hashMapUploadFinishTime;
    }

    public static void setHashMapUploadFinishTime(HashMap<Double, Edge> hashMapUploadFinishTime) {
        Channel.hashMapUploadFinishTime = hashMapUploadFinishTime;
    }

    public static HashMap<Double, Edge> getHashMapDownloadStartTime() {
        return hashMapDownloadStartTime;
    }

    public static void setHashMapDownloadStartTime(HashMap<Double, Edge> hashMapDownloadStartTime) {
        Channel.hashMapDownloadStartTime = hashMapDownloadStartTime;
    }

    public static HashMap<Double, Edge> getHashMapDownloadFinishTime() {
        return hashMapDownloadFinishTime;
    }

    public static void setHashMapDownloadFinishTime(HashMap<Double, Edge> hashMapDownloadFinishTime) {
        Channel.hashMapDownloadFinishTime = hashMapDownloadFinishTime;
    }


    public static double getTransferSpeed(){
        return transferSpeed;
    }

}
