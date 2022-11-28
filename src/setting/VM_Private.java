package setting;

import java.util.ArrayList;
import java.util.Objects;

public class VM_Private extends VM{
        public static  double LAUNCH_TIME = 0;

        //真实工作流上
        //public static  double NETWORK_SPEED = 10 * 1000*1000; // 默认为 20 MB

        public String ClassName = "PrivateVM";
        public static  int TYPE_NO = 9;
        public static  double[] SPEEDS = {1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5};    //L-ACO中用的
        //	public static final double[] SPEEDS = {0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5};
        public static  double[] UNIT_COSTS = {0.12, 0.195, 0.28, 0.375, 0.48, 0.595, 0.72, 0.855, 1};
        public static  double INTERVAL = 3600;    //one hour, billing interval

        public static  int FASTEST = 8;
        public static  int SLOWEST = 0;

        private int id;
        private int type;
        private ArrayList<Task> tasks;



        private double finalTaskFinishTime;

        public static int idInterval = 0;

        public VM_Private(int id, int type) {

                this.id = id;
                this.type = type;
        }

        //------------------------getters && setters---------------------------
        public double getSpeed() {
            return SPEEDS[0];
        }

        public double getUnitCost() {
            return UNIT_COSTS[type];
        }

        public Integer getId() {
            return id;
        }
        public void removeTask(Task task){
                tasks.remove(task);
        }
        public int getType() {
            return type;
        }

        public void setType(int type) {
            this.type = type;
        }

        public void setTasks(Task task){
                tasks.add(task);
        }

        public ArrayList<Task> getTasks(){
                return tasks;
        }

        public void setTasks(ArrayList<Task> tasks) {
                this.tasks = tasks;
        }

        public double getFinalTaskFinishTime() {
                return finalTaskFinishTime;
        }

        public void setFinalTaskFinishTime(double finalTaskFinishTime) {
                this.finalTaskFinishTime = finalTaskFinishTime;
        }



        //-------------------------------------overrides--------------------------------
        public String toString() {
            return "VM [" + id + ", type=" + type + "]";
        }


        @Override
        public boolean equals(Object o) {
                if (this == o) return true;
                if (o == null) return false;
                VM_Private that = (VM_Private) o;
                return id == that.id && type == that.type && Objects.equals(tasks, that.tasks);
        }

        @Override
        public int hashCode() {
                return Objects.hash(id, type, tasks);
        }

        public String getAttribute(){
                return "Private";
        }
}
