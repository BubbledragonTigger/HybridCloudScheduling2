package setting;

import java.util.ArrayList;
import java.util.Objects;

public class VM_Public extends VM{
    public double finalTaskFinishTime;
    public static final double LAUNCH_TIME = 0;
    /*
    public static final double NETWORK_SPEED = 12.5 * 1000*1000; // 默认为 20 MB

     */




    public static final int TYPE_NO = 9;
    public static final double[] SPEEDS = {1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5};	//L-ACO中用的
    //	public static final double[] SPEEDS = {0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5};
    public static final double[] UNIT_COSTS = {0.12, 0.195, 0.28, 0.375, 0.48, 0.595, 0.72, 0.855, 1};
    public static final double INTERVAL = 3600;	//one hour, billing interval

    public static final int FASTEST = 8;
    public static final int SLOWEST = 0;

    private int id = 0;

    private int type;
    public static int idInterval = 0;
    public VM_Public(int id, int type) {

        this.id = id;
        this.type = type;
    }
    public String ClassName = "PublicVM";
    //------------------------getters && setters---------------------------
    public double getSpeed() {
        return SPEEDS[8];
    }

    public double getUnitCost() {
        return UNIT_COSTS[type];
    }

    public Integer getId() {
        return id;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    //-------------------------------------overrides--------------------------------
    public String toString() {
        return "VM [" + id + ", type=" + type + "]";
    }

    private ArrayList<Task> tasks;
    public void setTasks(Task task){
        tasks.add(task);
    }
    public void removeTask(Task task){
        tasks.remove(task);
    }
    public void setTasks(ArrayList<Task> tasks) {
        this.tasks = tasks;
    }

    public ArrayList<Task> getTasks(){
        return tasks;
    }

    public double getFinalTaskFinishTime() {
        return finalTaskFinishTime;
    }

    public void setFinalTaskFinishTime(double finalTaskFinishTime) {
        this.finalTaskFinishTime = finalTaskFinishTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
       if (o == null ) return false;
        VM_Public vm_public = (VM_Public) o;
        return id == vm_public.id && type == vm_public.type && Objects.equals(tasks, vm_public.tasks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, tasks);
    }

    public String getAttribute(){
        return "Public";
    }
}
