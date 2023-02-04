package setting;

import contentionAware.Channel;

import java.util.*;

public class Workflow {

    private List<Task> list;

    //sequentialLength is used to calculate Speedup
    private double sequentialLength, CCR;
    private double speedUp;

    private TProperties b_levels, s_Levels,PEFT_levels,c_levels;
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
        ProjectCofig.bound=list.size()-2;//减去入口和出口虚拟节点

        setPrivacy(list,ProjectCofig.betaType);  //设置隐私，在Workflow类里面设置，就无需在CCSH算法里面设置了。


        int maxOutd = 0;
        for (int i = 0; i < list.size(); i++) {

            Task t = list.get(i);
            if(t.getOutEdges().size()>maxOutd) maxOutd = t.getOutEdges().size();

        }

        //only used in Clevel
        Workflow.maxOutd = maxOutd;
        c_levels = new TProperties(this,ProjectCofig.type);
        Collections.sort(list, c_levels);
        Collections.reverse(list);
        System.out.println("topological sort and clevel");
        /*for (Task t : list)
            System.out.println(t.getName() + "\t" + c_levels.get(t));*/

        for(int i = 0;i< list.size();i++){
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


        //设置CCR
        setCCRInHybridCloud();
        //序列长度
        System.out.println("CCR is " + CCR);
        setSequentialLength();
        System.out.println("workflow's sequentialLength is " + sequentialLength);
        System.out.println("workflow's critial task path:"+ getCPTaskLength());
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

    private List<Task> setPrivacy(List<Task> tasks, int betaType){
        if(betaType == 0){
            //设置随机数B=0.1，如果任务大小是50，则设置其中5个任务为私有任务
            double beta = ProjectCofig.beta;
            Random ran = new Random();
            ran.setSeed(ProjectCofig.seed);
            Set<Integer> set = new TreeSet<>();
            while(true){
                int a = ran.nextInt(ProjectCofig.bound);
                set.add(a);
                int privacyMaxsize = (int)(tasks.size()*beta);
                if(set.size()>=privacyMaxsize){
                    break;
                }
            }

            for (int i = 0; i < tasks.size(); i++) {
                Task task = tasks.get(i);
                if(ProjectCofig.adaptorType!=2)task.setRunOnPrivateOrPublic(false);  //复制调度时此属性不能使用
                task.setPrivateAttribute(false);

                if(task.getName().equals("exit") || task.getName().equals("entry")) continue;
                Integer number=Integer.valueOf(task.getName().substring(2));
                if(set.contains(number)){
                    if(ProjectCofig.adaptorType!=2)task.setRunOnPrivateOrPublic(true);
                    task.setPrivateAttribute(true);
                }
            }
        }

        else if(betaType == 1){
            //对特定任务设置隐私
            for (int i = 0; i < tasks.size(); i++) {
                Task task = tasks.get(i);
                //task.setRunOnPrivateOrPublic(false);
                task.setPrivateAttribute(false);
    ////            if(task.getName().equals(("ID00001")) || task.getName().equals(("ID00002")) || task.getName().equals(("ID00049"))){
    ////                task.setRunOnPrivateOrPublic(true);
    ////            }
    //            //dot5
    ////            if(task.getName().equals(("1")) || task.getName().equals(("2")) || task.getName().equals(("9"))){
    ////                task.setRunOnPrivateOrPublic(true);
    ////            }
    //            //dot6
                /*if( task.getName().equals(("1")) || task.getName().equals(("2")) || task.getName().equals(("9")) || task.getName().equals(("8"))){
                    task.setRunOnPrivateOrPublic(true);
                }*/
                //dot8
                /*if( task.getName().equals(("6")) || task.getName().equals(("8"))||task.getName().equals(("1"))) {
                    //task.setRunOnPrivateOrPublic(true);
                    task.setPrivateAttribute(true);
                }*/
                //dot9
                if( task.getName().equals(("10")) || task.getName().equals(("8"))||task.getName().equals(("2"))) {
                    task.setRunOnPrivateOrPublic(true);
                    task.setPrivateAttribute(true);
                }

            }
        }
        return tasks;
    }

    //混合云中计算sequentialLength
    public void setSequentialLength(){
        sequentialLength = 0;
        for(Task task : list){
            if(task.getprivateAttribute()==true){
                sequentialLength +=task.getTaskSize()/VM_Private.SPEEDS[VM_Private.SLOWEST];
            }
            else{
                sequentialLength += task.getTaskSize()/VM_Public.SPEEDS[VM_Public.FASTEST];
            }
        }
        return;
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

    public double getCCR() {	return CCR;	}
    public double getCPLength() {
        return b_levels.get(this.getEntryTask());
    }
    public double getCPTaskLength() {
        if(s_Levels == null)
            s_Levels = new TProperties(this, TProperties.Type.S_LEVEL);
        return s_Levels.get(this.getEntryTask());
    }

    //only used in Clevel.Caculate max out degree of the graph
    public static int maxOutd=0;



    public double getSequentialLength(){
        return sequentialLength;
    }

    //local test
    public static void main(String[] args){
        //F:\\dax\\CYBERSHAKE\\CYBERSHAKE.n.100.1.dax
        //D:\\test1.dot		D:\\seismology-workflow.json
        Workflow wf = new Workflow("D:\\example.dot");
//		WorkflowParser.exportToDOTFile(wf, "D:\\Genome-50.dot");
//		GraphViz.convertDOTFileToImageFile("D:\\Genome-50.dot", "png", "D:\\SoyKB-50.png");
    }

    //混合云环境下的CCR
    private void setCCRInHybridCloud(){
        double sensitiveTaskSizeSum = 0,insensitiveTaskSizeSum=0, edgeSizeSum = 0;
        int sensitiveTaskNumber=0,insensitiveTaskNumber=0;
        int edgeNum = 0;
        for (Task task : list) {
            if(task.getprivateAttribute()==true){
                sensitiveTaskSizeSum+=task.getTaskSize();
                sensitiveTaskNumber++;
            }
            else{
                insensitiveTaskSizeSum+=task.getTaskSize();
                insensitiveTaskNumber++;
            }
            for (Edge outEdge : task.getOutEdges()) {
                edgeSizeSum += outEdge.getDataSize();
                edgeNum++;
            }
        }
        CCR = edgeSizeSum / edgeNum / Channel.getTransferSpeed();
        System.out.println((sensitiveTaskSizeSum / VM_Private.SPEEDS[VM_Private.SLOWEST])/sensitiveTaskNumber);
        CCR = CCR / ((sensitiveTaskSizeSum / VM_Private.SPEEDS[VM_Private.SLOWEST])/sensitiveTaskNumber
                +(insensitiveTaskSizeSum/VM_Public.SPEEDS[VM_Public.FASTEST])/insensitiveTaskNumber);
    }

    //单一云环境下的CCR
    private void setCCRInSingleCloud(){
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
        CCR = edgeSizeSum / edgeNum / VM.NETWORK_SPEED;
        CCR = CCR / (taskSizeSum / list.size() / VM.SPEEDS[VM.FASTEST]);
    }



}
