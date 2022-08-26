package setting;

import java.util.ArrayList;

public  class  VM {
    public static int idPrivateInterval = 0;
    public static int idPublicInterval = 0;

    public ArrayList<Task> getTasks() {
        return tasks;
    }

    public void setTasks(ArrayList<Task> tasks) {
        this.tasks = tasks;
    }

    private ArrayList<Task> tasks;
    //改
    public static double NETWORK_SPEED = 10 * 1000 * 1000;
    //public static double NETWORK_SPEED = 50 ;

    public static  int FASTEST = 8;
    public static  int SLOWEST = 0;
    public static  double LAUNCH_TIME = 0;
    public static  double INTERVAL = 3600;	//one hour, billing interval
    public static  double[] SPEEDS = {1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5};	//L-ACO中用的




    public static  int TYPE_NO = 9;

    //	public static final double[] SPEEDS = {0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5};
    public static  double[] UNIT_COSTS = {0.12, 0.195, 0.28, 0.375, 0.48, 0.595, 0.72, 0.855, 1};


    private int id;
    private int type;
    //private ArrayList<Task> tasks;

    public VM(){};
    //only invoked by Solution, as id only make senses in one Solution
    public VM(int id, int type){
        this.id = id;
        this.type = type;
    }






    //------------------------getters && setters---------------------------
    public double getSpeed(){		return SPEEDS[type];	}
    public double getUnitCost(){		return UNIT_COSTS[type];	}
    public int getId() {		return id;	}
    public int getType() {		return type;	}
    public void setType(int type) {			this.type = type;	}




    //-------------------------------------overrides--------------------------------
    public String toString() {
        return "VM [" + id + ", type=" + type + "]";
    }



}
