package setting;

import java.util.*;

public class Workflow {

    private List<Task> list;	//按照blevel排序

    private double sequentialLength, CCR;
    private TProperties b_levels, s_Levels,PEFT_levels;
    //used to accelerate searching a task index in the workflow
    private HashMap<Task, Integer> taskIndices = new HashMap<>();

    /**
     * construct a workflow according to @param file
     * two kinds of formats are supported: 1. DOT graphs; 2. Pegasus dax files
     */

    public void addTask(Task task){
        list.add(task);
        list = topoSort(list.get(0));
    }
    public Workflow(String file) {
        try {
            if (file.endsWith("xml") || file.endsWith("dax"))
                list = WorkflowParser.parseXMLFile(file);
            else if (file.endsWith("dot"))
                list = WorkflowParser.parseDOTFile(file);
            else
                list = setting.WorkflowParser2.parseJSONFile(file);
        } catch (Exception e) {    // convert non-RuntimeExceptions to RuntimeException
            throw new RuntimeException("fails to parse " + file);
        }
        System.out.println("succeed to read a workflow from " + file);
        list = topoSort(list.get(0));   //为什么先拓扑排序后按gamma排序，直接gamma排序不行吗。后续懂了：gamma排序也是基于拓扑排序
        //sort based on b_level, can only be invoked after the topoSort method



        b_levels = new TProperties(this,TProperties.Type.PEFT);
        Collections.sort(list, b_levels);

        Collections.reverse(list);
        System.out.println("topological sort and blevel：");
//        for (Task t : list)
//            System.out.println(t.getName() + "\t" + b_levels.get(t));
        for (int i = 0; i < list.size(); i++) {
            Task t = list.get(i);
            taskIndices.put(t, i);
        }

        //sort edges for each task,就是按照节点的优先级排序的，优先级高的排在前面
        class EComparator implements Comparator<Edge> {
            boolean isDestination;    // if true, compare destinations; otherwise, compare sources

            public EComparator(boolean isDestination) {
                this.isDestination = isDestination;
            }

            public int compare(Edge o1, Edge o2) {
                Task task1 = isDestination ? o1.getDestination() : o1.getSource();
                Task task2 = isDestination ? o2.getDestination() : o2.getSource();
                return Integer.compare(taskIndices.get(task1), taskIndices.get(task2));
                //如果前面的小于后面的，则返回-1，小的在前
            }
        }
        ;
        /*
        for (Task t : list) {
            t.sortEdges(Task.TEdges.IN, new EComparator(false));
            t.sortEdges(Task.TEdges.OUT, new EComparator(true));
        }    //sort edges for each task，这个排序就是按gamma来的，优先级高的在前面，优先级低的在后面


         */
        double taskSizeSum = 0, edgeSizeSum = 0;
        int edgeNum = 0;
        for (Task task : list) {
            taskSizeSum += task.getTaskSize();
            for (Edge outEdge : task.getOutEdges()) {
                edgeSizeSum += outEdge.getDataSize();
                edgeNum++;
            }
        }
        //需要后期改
        /*
        CCR = edgeSizeSum / edgeNum / VM.NETWORK_SPEED;
        CCR = CCR / (taskSizeSum / list.size() / VM.SPEEDS[VM.FASTEST]);
        sequentialLength = taskSizeSum / VM.SPEEDS[VM.FASTEST];
        //序列长度
        System.out.println("CCR is " + CCR);
        */

    }

    private List<Task> topoSort(Task entry) {
        // Empty list that will contain the sorted elements
        final List<Task> topoList = new ArrayList<Task>();
        List<Task> S = new ArrayList<Task>();	//S←Set of all nodes with no incoming edges
        S.add(entry);

        HashMap<Task, Integer> inEdgeCounts = new HashMap<>();//用来存储每个任务的输入边的个数
        while(S.size()>0){
            Task task = S.remove(0);		// remove a node n from S
            topoList.add(task);			// add n to tail of L
            for(Edge e : task.getOutEdges()){	// for each node m with an edge e from n to m do
                Task t = e.getDestination();
                Integer count = inEdgeCounts.get(t);
                inEdgeCounts.put(t, count != null ? count+1 : 1);
                if(inEdgeCounts.get(t) == t.getInEdges().size())	//if m has no other incoming edges then
                    S.add(t);					// insert m into S
            }
        }
        return topoList;
    }



    //--------------------------getters-----------------------------------------
    public int size(){
        return list.size();
    }
    public Task get(int index){
        return list.get(index);
    }
    public Task getEntryTask(){
        return list.get(0);
    }
    public Task getExitTask(){
        return list.get(list.size()-1);
    }
    public int indexOf(Task task){
        if(taskIndices == null || taskIndices.get(task) == null)
            return list.indexOf(task);
        else
            return taskIndices.get(task);
    }
    public List<Task> getTaskList(){
        return Collections.unmodifiableList(list);
    }
    public double getSequentialLength() {	return sequentialLength;	}
    public double getCCR() {	return CCR;	}
    public double getCPLength() {
        return b_levels.get(this.getEntryTask());
    }
    public double getCPTaskLength() {
        if(s_Levels == null)
            s_Levels = new TProperties(this, TProperties.Type.S_LEVEL);
        return s_Levels.get(this.getEntryTask());
    }


    //local test
    public static void main(String[] args){
        //F:\\dax\\CYBERSHAKE\\CYBERSHAKE.n.100.1.dax
        //D:\\test1.dot		D:\\seismology-workflow.json
        Workflow wf = new Workflow("D:\\example.dot");
//		WorkflowParser.exportToDOTFile(wf, "D:\\Genome-50.dot");
//		GraphViz.convertDOTFileToImageFile("D:\\Genome-50.dot", "png", "D:\\SoyKB-50.png");
    }





}
