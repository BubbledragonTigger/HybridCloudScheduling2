package contentionAware;

import java.util.*;
import java.util.concurrent.ExecutionException;

import com.sun.xml.internal.bind.v2.runtime.reflect.Lister;
import contentionFree.Allocation;
import contentionFree.LSUtil;
import contentionFree.TAllocation;
import setting.*;

import static java.lang.Math.max;

public class CCSH {
    private CSolution csolution;
    private int publicVMMaxNuma;

    private int test;aaaasssssssssssssss
    //记录边分配策略
    private ArrayList<EAllocation> eAllocationList = new ArrayList<>();

    //记录任务分配策略
    private ArrayList<TAllocation> tAllocationList = new ArrayList<>();

    //记录通道分配策略
    private ArrayList<CAllocation> cAllocationList = new ArrayList<>();

    /**
     * static list scheduling for @param wf
     *
     * @param type = 0: no ne
     *             type = 1: rescheduling is enabled
     *             type = 2: duplication is supported
     *             besides, insert is always supported
     */

    public CSolution listSchedule(Workflow wf, TProperties.Type tctype, int type) {


        ArrayList<Task> tasks = new ArrayList<>(wf.getTaskList());

        //在传入之前已经拓扑排序过了
        Collections.sort(tasks, new TProperties(wf, tctype));
        Collections.reverse(tasks);


        //创建私有云公有云服务器列表
        ArrayList<VM_Private> vmPrivateList = new ArrayList<>();
        ArrayList<VM_Public> vmPublicList = new ArrayList<>();


        //csolution = new CSolution();
        for (int i = 0; i < tasks.size(); i++) {

            Task task = tasks.get(i);

            //准备Candidate
            vmPublicList = getPreparePublicVMCandidate(vmPublicList, type);
            vmPrivateList = getPreparePrivateVMCandidate(vmPrivateList, type);

            //更好的编码，后期测试可不可以使用
            //vmPublicList = getCandidate( vmPublicList , type ,true);
            //vmPrivateList = getCandidate( vmPrivateList , type ,false);

            //用来记录
            double minEST = Double.MAX_VALUE;
            double minEFT = Double.MAX_VALUE;
            VM_Private selectedPrivateVM = null;
            VM_Public selectedPublicVM = null;
            //记录 allocateIncomingEdges中永久分配的边和通道
            ArrayList<EAllocation> eAllocationList = new ArrayList<>();
            ArrayList<CAllocation> cAllocationList = new ArrayList<>();

            //当前任务ni是私密任务true:私有云；false:公有云:
            if (task.getRunOnPrivateOrPublic() == true) {
                for (VM_Private vm : vmPrivateList) {
                    double tsTask;  //Task EST
                    double tfTask;  //Task EFT

                    tsTask = calcTSTaskWithInsert(task, null, vm, 1);
                    tfTask = tsTask + task.getTaskSize()/vm.getSpeed();
                    if(tctype == TProperties.Type.PEFT)  //PEFT要让tfTASK额外加上OCT，
                    {
                        tfTask = tfTask + task.getOCT()[0];  //私有云上
                    }else if(tctype == TProperties.Type.IPPTS)
                    {
                        double LHET = task.getPCM()[0]-task.getTaskSize()/vm.getSpeed();                  //Compute LHET
                        double Lhead = tfTask + LHET;
                        tfTask = Lhead;
                    }else if(tctype == TProperties.Type.C_LEVEL)
                    {
                        double dft = dFT(task,vm);
                        tfTask = tfTask + dft;
                    }


                    //针对于HEFT中，minEFT代表的是最早完成时间，在IPPTS,PEFT,CCSH中不是代表最早完成时间
                    if(minEFT>tfTask){
                        minEST = tsTask;
                        minEFT = tfTask;
                        selectedPrivateVM = vm;
                        selectedPublicVM = null;
                        task.setPriavteVM(vm);
                        task.setPublicVM(null);
                        task.setRunOnPrivateOrPublic(true);
                    }


                }

            } else {
                for (VM_Public vm : vmPublicList) {
                    double tsTask;
                    double tfTask;

                    tsTask = calcTSTaskWithInsert(task, vm, null, 1);
                    tfTask = tsTask + task.getTaskSize()/vm.getSpeed();
                    if(tctype == TProperties.Type.PEFT)  //PEFT要让tfTASK额外加上OCT，
                    {
                        tfTask = tfTask + task.getOCT()[1];  //公有云上
                    }else if(tctype == TProperties.Type.IPPTS)
                    {
                        double LHET = task.getPCM()[1]-task.getTaskSize()/vm.getSpeed();                  //Compute LHET
                        double Lhead = tfTask + LHET;
                        tfTask = Lhead;
                    }else if(tctype == TProperties.Type.C_LEVEL)
                    {
                        double dft = dFT(task,vm);
                        tfTask = tfTask + dft;
                    }

                    if(minEFT>tfTask){
                        minEST = tsTask;
                        minEFT = tfTask;
                        selectedPrivateVM = null;
                        selectedPublicVM = vm;
                        task.setPriavteVM(null);
                        task.setPublicVM(vm);
                        task.setRunOnPrivateOrPublic(false);
                    }


                }
                for (VM_Private vm : vmPrivateList) {
                    double tsTask;
                    double tfTask;

                    tsTask = calcTSTaskWithInsert(task, null, vm, 1);
                    tfTask = tsTask + task.getTaskSize()/vm.getSpeed();
                    if(tctype == TProperties.Type.PEFT)  //PEFT要让tfTASK额外加上OCT，
                    {
                        tfTask = tfTask + task.getOCT()[0];  //私有云上
                    }else if(tctype == TProperties.Type.IPPTS)
                    {
                        double LHET = task.getPCM()[0]-task.getTaskSize()/vm.getSpeed();                  //Compute LHET
                        double Lhead = tfTask + LHET;
                        tfTask = Lhead;
                    }else if(tctype == TProperties.Type.C_LEVEL)
                    {
                        double dft = dFT(task,vm);
                        tfTask = tfTask + dft;
                    }

                    if(minEFT>tfTask){
                        minEST = tsTask;
                        minEFT = tfTask;
                        selectedPrivateVM = vm;
                        selectedPublicVM = null;
                        task.setPriavteVM(vm);
                        task.setPublicVM(null);
                        task.setRunOnPrivateOrPublic(true);
                    }
                }
            }
            if(selectedPrivateVM != null){
                addTaskEdgesToVM(null,selectedPrivateVM, task, minEST);
            }
            else{
                addTaskEdgesToVM(selectedPublicVM,null, task, minEST);
            }

        }
        System.out.println("--------");
        ChannelSolution channelSolution = new ChannelSolution(eAllocationList,cAllocationList,tAllocationList);
        if(type == 1){ //基于共享，之前没有用到csolution,此函数专门来进行赋值操作
            /*想法有问题
           // csolution = transformCSlotion(tasks,wf);  想法有问题
            //contentionAware.Adaptor adaptor = new contentionAware.Adaptor();
            //return adaptor.buildFromSolutionShared(csolution, wf);

             */
            contentionAware.ChannelAdaptor channelAdaptor = new ChannelAdaptor();
            channelAdaptor.buildFromSolutionShared(channelSolution,wf);

        }
        System.out.println("e:"+channelSolution.gettAllocations().get(channelSolution.gettAllocations().size()-1).getFinishTime());
        return csolution;

    }

    private CSolution transformCSlotion(ArrayList<Task> tasks,Workflow wf){
        csolution = new CSolution();
        csolution.newVM(VM.SLOWEST);  //私有云内部速度是1
        csolution.newVM(VM.FASTEST);  //公有云内部速度是5

        List<VM> vmList = new ArrayList<>(csolution.getUsedVMSet());  //vm1是私有云，vm2是公有云
        Collections.sort(vmList, new Comparator<VM>(){
            public int compare(VM v1, VM v2) {
                if(v1.getType() == 0)return -1;
                if(v2.getType() == 0) return 1;
                return 0;
            }
        });

        //将task和VM联系在一起
        for(Task task : tasks){
            if(task.getRunOnPrivateOrPublic() == true){
                task.setVM(vmList.get(0));
            }
            else{
                task.setVM(vmList.get(1));

            }
        }



        ArrayList<EAllocation> adaptorEAllocationList = new ArrayList<>();
        ArrayList<EAllocation> finalAdaptorEAllocationList = new ArrayList<>();

        for(EAllocation eAllocation: eAllocationList){

            //这一步是改造eAllocation,因为之前都是用PrivateVM和PublicVM，现在改成VM1,VM2
            Edge edge = eAllocation.getEdge();

            if(edge.getSource().getRunOnPrivateOrPublic()== true && edge.getDestination().getRunOnPrivateOrPublic()== false){
                EAllocation eAllocation1 = new EAllocation(edge,vmList.get(0),vmList.get(1),eAllocation.getStartTime(),Channel.getTransferSpeed(),eAllocation.getFinishTime());
                finalAdaptorEAllocationList.add(eAllocation1);
            }

            if(edge.getSource().getRunOnPrivateOrPublic()== false && edge.getDestination().getRunOnPrivateOrPublic()== true){
                EAllocation eAllocation1 = new EAllocation(edge,vmList.get(1),vmList.get(0),eAllocation.getStartTime(),Channel.getTransferSpeed(),eAllocation.getFinishTime());
                finalAdaptorEAllocationList.add(eAllocation1);
            }



            boolean flag = false;
            for(CAllocation cAllocation: cAllocationList){
                if(cAllocation.containsEAllocation(eAllocation)){
                    flag = true;
                }
            }
            if(flag == true){
                adaptorEAllocationList.add(eAllocation);

            }
        }
        int idNum=1;
        //改造dot图，将边改为任务
        String taskAdaptorName = Integer.toString(tasks.size()-2+idNum);  //-2是入口出口任务占两个
        for(EAllocation eAllocation: eAllocationList){
            if(!adaptorEAllocationList.contains(eAllocation)){


                Edge edge = eAllocation.getEdge();




                //设置task
                double taskSize = edge.getDataSize() / VM.NETWORK_SPEED;
                Task task = new Task(taskAdaptorName,taskSize);
                idNum++;

                taskAdaptorName = Integer.toString(tasks.size()-2+idNum);

                edge.setDataSize(0);  //原边变成虚边

                Task sourceTask = edge.getSource();

                task.setAST(sourceTask.getAFT());
                if(edge.getSource().getRunOnPrivateOrPublic() == true){  //都在私有云上执行
                    task.setVM(vmList.get(0));
                    task.setAFT(task.getAST()+ taskSize/VM_Private.SPEEDS[VM_Private.SLOWEST]);
                    tasks.add(task);

                }
                else{   //都在公有云上执行
                    task.setVM(vmList.get(1));
                    task.setAFT(task.getAST()+ taskSize/VM_Public.SPEEDS[VM_Private.FASTEST]);
                    tasks.add(task);
                }

                Edge edge1 = new Edge(sourceTask,task);
                edge1.setDataSize(0);
                Task destTask = edge.getDestination();
                Edge edge2 = new Edge(task,destTask);
                edge2.setDataSize(0);
                sourceTask.insertEdge(Task.TEdges.OUT,edge1);
                destTask.insertEdge(Task.TEdges.IN,edge2);
                task.insertEdge(Task.TEdges.IN,edge1);
                task.insertEdge(Task.TEdges.OUT,edge2);
                wf.addTask(task);
                EAllocation eAllocation1 = new EAllocation(edge1,sourceTask.getVM(), task.getVM(),sourceTask.getAFT(),0,sourceTask.getAFT());
                EAllocation eAllocation2 = new EAllocation(edge2,task.getVM(), destTask.getVM(),task.getAFT(),0,task.getAFT());
                finalAdaptorEAllocationList.add(eAllocation1);
                finalAdaptorEAllocationList.add(eAllocation2);
            }
        }
        for(EAllocation eAllocation: finalAdaptorEAllocationList){
            csolution.addEdge(eAllocation);
        }
        for(Task task :tasks){
            csolution.addTaskToVM(task.getVM(),task,task.getAST());
        }





        return csolution;
    }

    private ArrayList<Task> setPrivacy(ArrayList<Task> tasks){

        //设置随机数B=0.1，如果任务大小是50，则设置其中5个任务为私有任务
//        double beta = 0.1;
//        Random ran = new Random();
//        ran.setSeed(3);
//        Set<Integer> set = new TreeSet<>();
//        while(true){
//            int a = ran.nextInt(50);
//            set.add(a);
//            int privacyMaxsize = (int)(tasks.size()*beta);
//            if(set.size()>=privacyMaxsize){
//                break;
//            }
//        }
//
//        for (int i = 0; i < tasks.size(); i++) {
//            Task task = tasks.get(i);
//            task.setRunOnPrivateOrPublic(false);
//
//            if(task.getName().equals("exit") || task.getName().equals("entry")) continue;
//            Integer number=Integer.valueOf(task.getName().substring(2));
//            if(set.contains(number)){
//                task.setRunOnPrivateOrPublic(true);
//            }
//        }


          //对特定任务设置隐私
        for (int i = 0; i < tasks.size(); i++) {
            Task task = tasks.get(i);
            task.setRunOnPrivateOrPublic(false);
//            if(task.getName().equals(("ID00001")) || task.getName().equals(("ID00002")) || task.getName().equals(("ID00049"))){
//                task.setRunOnPrivateOrPublic(true);
//            }
            //dot5
//            if(task.getName().equals(("1")) || task.getName().equals(("2")) || task.getName().equals(("9"))){
//                task.setRunOnPrivateOrPublic(true);
//            }
            //dot6
            if( task.getName().equals(("1")) || task.getName().equals(("2")) || task.getName().equals(("9"))){
                task.setRunOnPrivateOrPublic(true);
            }
        }
        return tasks;
    }
    //private transformCAllocationToCSolution

    private  List<Allocation> addTaskEdgesToVM(VM_Public publicVM,VM_Private privateVM , Task task, double startTime) {
        List<Allocation> list = new ArrayList<>();
        //先判断是添加publicVM,还是privateVM,如果当前VM还没有分配Task，那么需要初始化Task列表
        if(publicVM != null){
            if (publicVM.getTasks() == null || publicVM.getTasks().size() == 0) {
                publicVM.setTasks(new ArrayList<Task>());
            }
            publicVM.setTasks(task);
            task.setAST(startTime);
            task.setAFT(startTime + task.getTaskSize()/publicVM.getSpeed());

            allocateIncomingEdges(task,publicVM,2);//step=2是不进行回滚操作
            TAllocation tAllocation = new TAllocation(publicVM,null,task,startTime,publicVM.getSpeed());
            tAllocationList.add(tAllocation);
            list.add(tAllocation);
            list.addAll(cAllocationList);

        }
        else{
            if (privateVM.getTasks() == null || privateVM.getTasks().size() == 0) {
               privateVM.setTasks(new ArrayList<Task>());
            }
            privateVM.setTasks(task);
            task.setAST(startTime);
            task.setAFT(startTime + task.getTaskSize()/privateVM.getSpeed());
            allocateIncomingEdges(task,privateVM,2);//step=2是不进行回滚操作
            TAllocation tAllocation = new TAllocation(null,privateVM,task,startTime,privateVM.getSpeed());
            tAllocationList.add(tAllocation);
            list.add(tAllocation);
            list.addAll(cAllocationList);

        }
        return list;

    }

    private double calcTSTaskWithInsert(Task task, VM_Public publicVM, VM_Private privateVM, int step) {
        if (publicVM != null) {
            double DAT = allocateIncomingEdges(task, publicVM, step);

            //改速度
            double period = task.getTaskSize() / publicVM.getSpeed();
            List<TAllocation> allocs = new ArrayList<TAllocation>();

            for (TAllocation tAllocation : tAllocationList) {
                if (tAllocation.getPublicVM() == publicVM) {
                    allocs.add(tAllocation);
                }
            }
            double EST = searchFreeTimeSlot(allocs, DAT, period);
            return EST;
        } else {
            double DAT = allocateIncomingEdges(task, privateVM, step);
            double period = task.getTaskSize() / privateVM.getSpeed();
            List<TAllocation> allocs = new ArrayList<TAllocation>();

            for (TAllocation tAllocation : tAllocationList) {
                if (tAllocation.getPrivateVM() == privateVM) {
                    allocs.add(tAllocation);
                }
            }
            double EST = searchFreeTimeSlot(allocs, DAT, period);
            return EST;
        }

    }

    /**
     * Search the earliest time slot in @param allocList from @param readyTime,
     * which is no less than @param period
     * 在allocList上从readytime开始，寻找最早的支持period长度的free time slot
     */

    private static double searchFreeTimeSlot(List<? extends Allocation> allocList,
                                             double readyTime, double period) {
        if (allocList == null || allocList.size() == 0)
            return readyTime;
        Collections.sort(allocList, new Comparator<Allocation>() {
            @Override
            public int compare(Allocation o1, Allocation o2) {
                double startTime1 = o1.getStartTime();
                double startTime2 = o2.getStartTime();
                return Double.compare(startTime1, startTime2);
            }
        });
        // case1: 插入在最前面，
        if (readyTime + period <= allocList.get(0).getStartTime()) {
            return readyTime;
        }
        double EST = 0;
        for (int j = allocList.size() - 1; j >= 0; j--) {
            Allocation alloc = allocList.get(j);//放在alloc的后面
            double startTime = max(readyTime, alloc.getFinishTime());   //
            double finishTime = startTime + period;

            if (j == allocList.size() - 1) {            // case2: 放在最后面，EST肯定可以传输边，在慢慢寻找间隙
                EST = startTime;             //EST也是处理器最早可以传输边的时间
            } else {  //case3: 插入到中间。alloc不是最后一个，还存在allocNext；需test能否插入
                Allocation allocNext = allocList.get(j + 1);
                if (finishTime > allocNext.getStartTime())//就是结束的时间大于了后面另一条边开始传输的时间，存在overlap无法插入
                    continue;

                EST = startTime;//否则就可以,就是结束的时间小于了后面另一条边开始传输的时间
            }
            if (readyTime > alloc.getFinishTime())
                break;
        }
        return EST;

    }

    //准备公有云上候选集
    public ArrayList<VM_Public> getPreparePublicVMCandidate(ArrayList<VM_Public> vmPublicList, int type) {
        boolean emptyFlag = false;
        for (VM_Public vm : vmPublicList) {
            if(vm.getTasks()==null){
                vm.setTasks(new ArrayList<Task>());
            }
            if (vm.getTasks().size() == 0 ) {
                emptyFlag = true;
                break;
            }
        }

        if (vmPublicList.size() == 0 || emptyFlag == false) {
            VM_Public vm_public = new VM_Public(++VM_Public.idInterval, type);
            vmPublicList.add(vm_public);
        }
        //去除null，防止排序时报空指针异常
        ArrayList<VM_Public> Nonlist = new ArrayList<>();
        Nonlist.add(null);
        vmPublicList.removeAll(Nonlist);

        Collections.sort(vmPublicList, new Comparator<VM_Public>() {
            @Override
            public int compare(VM_Public v1, VM_Public v2) {
                if ( v1.getTasks() == null || v1.getTasks().size() == 0 ) {
                    return 1;
                }
                if ( v2.getTasks() == null || v2.getTasks().size() == 0 ) {
                    return -1;
                }
                return 0;
            }
        });  //空的vm放后面

        return vmPublicList;
    }


    //准备私有云上候选集
    public ArrayList<VM_Private> getPreparePrivateVMCandidate(ArrayList<VM_Private> vmPrivateList, int type) {
        //int maxNum =

        boolean emptyFlag = false;
        for (VM_Private vm : vmPrivateList) {
            if(vm.getTasks()==null){
                vm.setTasks(new ArrayList<Task>());
            }
            if (vm.getTasks().size() == 0) {
                emptyFlag = true;
                break;
            }
        }

        if (vmPrivateList.size() == 0 || emptyFlag == false) {
            VM_Private vm_private = new VM_Private(++(VM_Private.idInterval), type);
            vmPrivateList.add(vm_private);
        }

        //去除null，防止排序时报空指针异常
        ArrayList<VM_Private> Nonlist = new ArrayList<>();
        Nonlist.add(null);
        vmPrivateList.removeAll(Nonlist);

        Collections.sort(vmPrivateList, new Comparator<VM_Private>() {
            @Override
            public int compare(VM_Private v1, VM_Private v2) {
                if ( v1.getTasks() == null ||v1.getTasks().size() == 0 ) {
                    return 1;
                }
                if (v2.getTasks() == null ||v2.getTasks().size() == 0 ) {
                    return -1;
                }
                return 0;
            }
        });  //空的vm放后面
        return vmPrivateList;
    }


    //Algorithm 2 ALLocateIncomingEdges(n$_j$,v$_l$),私有云上
    public double allocateIncomingEdges(Task nj, VM_Private vl, int step) {
        /*
        nj:当前任务
        vl:处理器
        step:如果为1则进行是虚假分配，即要进行回滚操作，如果为2则进行真正的分配
         */
        List<Edge> inEdges = new ArrayList<Edge>(nj.getInEdges());

        //line 1
        Collections.sort(inEdges, new Comparator<Edge>() {
            @Override
            public int compare(Edge e1, Edge e2) {
                if (e1.getSource().getTaskSize() == 0 && e1.getSource().getInEdges() == null)  //entry
                    return 1;
                if (e2.getSource().getTaskSize() == 0 && e2.getSource().getInEdges() == null)  //entry
                    return -1;

                double eReadyTime1 = getMinEReadyTime(e1, vl);    //ready time
                double eReadyTime2 = getMinEReadyTime(e2, vl);
                //论文中算法2里的ts(ei,j, μ(ni));的计算就是和这里一样的，也是暗含插入
                return Double.compare(eReadyTime1, eReadyTime2);
            }
        });//按照源的DAT升序排序InEdges；	parentTask在该vm上的edge，排在哪里都无所谓

        //Line2
        double DAT = 0;

        //记录临时的分配边和分配的通道
        ArrayList<EAllocation> eAllocationListRecord = new ArrayList<>();
        ArrayList<CAllocation> cAllocationListRecord = new ArrayList<>();


        //Line3
        for (Edge inEdge : inEdges) {
            Task parentTask = inEdge.getSource();

            //entryTask
            if (parentTask.getTaskSize() == 0
                    && (parentTask.getInEdges() ==null
                    ||parentTask.getInEdges().size() == 0))  //entry
                continue;

            //exitTask
            if (parentTask.getTaskSize() == 0 && parentTask.getOutEdges() == null)
                continue;

            //(1)Line4
            if (parentTask.getPriavteVM() == vl && parentTask.getRunOnPrivateOrPublic() == true) {

                double eReadyTime = parentTask.getAFT();  //同一个处理器上我们也记录边分配
                double eFinishTime = eReadyTime ;
                EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPriavteVM(),
                        vl, eReadyTime, VM_Private.NETWORK_SPEED, eFinishTime);
                this.eAllocationList.add(eAllocation);
                eAllocationListRecord.add(eAllocation);
                DAT = Math.max(DAT, parentTask.getAFT());  //line5
            }

            //(2)Line6
            else if (parentTask.getPriavteVM() != vl && parentTask.getRunOnPrivateOrPublic() == true) {
                //Line7
                double eReadyTime = parentTask.getAFT();
                double eFinishTime = eReadyTime + inEdge.getDataSize() / VM_Private.NETWORK_SPEED;

                //line8
                EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPriavteVM(),
                        vl, eReadyTime, VM_Private.NETWORK_SPEED, eFinishTime);
                this.eAllocationList.add(eAllocation);
                eAllocationListRecord.add(eAllocation);

                //Line9
                DAT = Math.max(DAT, eFinishTime);
            }

            //(3)Line10
            else {
                //因为vl已经是私有云上服务器，所以只需要判断ui是否是公有云处理器
                if (parentTask.getRunOnPrivateOrPublic() == false) { //Line11
                    ArrayList<Double> downloadStartTimeList = new ArrayList<>();
                    ArrayList<Double> downloadFinishTimeList = new ArrayList<>();
                    for (CAllocation cAllocation : cAllocationList) {
                        if (cAllocation.getFlag() == false) { //false就是代表从公有云到私有云
                            downloadStartTimeList.add(cAllocation.getChannelDownloadStartTime());
                            downloadFinishTimeList.add(cAllocation.getChannelDownloadFinishTime());
                        }
                    }
                    Collections.sort(downloadStartTimeList);   //从小到大排序
                    Collections.sort(downloadFinishTimeList);

                    boolean isFinal = true;//如果为true就说明到最后也插入不进去了，直接放在
                    //逻辑是插入在当前上传任务的完成时间和下一个上传任务的开始时间之间

                    for (int i = 0; i < downloadStartTimeList.size(); i++) {
                        double downloadStartTime = downloadStartTimeList.get(i);
                        //这个判断是为了判断如果输入边的父任务在下一个上传阶段开始时间的时候还没有完成，则可以直接跳过此阶段
                        if (parentTask.getAFT() >= downloadStartTimeList.get(i)) {
                            continue;
                        }

                        //插入在开头，即所有下载边之前
                        if (i == 0) {
                            if (parentTask.getAFT() + inEdge.getDataSize() / Channel.getTransferSpeed() <= downloadStartTime) {
                                //此时边准备好了就可以传入了
                                double eReadyTime = parentTask.getAFT();
                                double eFinishTime = eReadyTime + inEdge.getDataSize() / Channel.getTransferSpeed();

                                double channelDownloadStartTime = eReadyTime;
                                double channelDownloadFinishTime = eFinishTime;

                                CAllocation c = new CAllocation(inEdge, parentTask.getPublicVM(),
                                        vl,  eReadyTime, eFinishTime, -1, -1,
                                        channelDownloadStartTime, channelDownloadFinishTime, false);
                                //永久分配，最后通过cAllocationListRecord中的记录撤销
                                cAllocationList.add(c);
                                //暂时分配
                                cAllocationListRecord.add(c);

                                //永久分配,这个EAllocation可能以后对画图有帮助，这里暂时记录，其实CAllocation已经包括了,参数VM_Private.NETWORK_SPEED用不到
                                EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPublicVM(),
                                        vl, eReadyTime, VM_Private.NETWORK_SPEED, eFinishTime);
                                this.eAllocationList.add(eAllocation);
                                eAllocationListRecord.add(eAllocation);
                                isFinal = false;
                                DAT = Math.max(DAT, eFinishTime);
                                break;
                            }
                        } else {
                            //前一个任务的完成时间，这里i永远不会为0，所以不会出错
                            double downloadPreviousFinishTime = downloadFinishTimeList.get(i - 1);
                            double eReadyTime = Math.max(downloadPreviousFinishTime, parentTask.getAFT());
                            //父任务的完成时间>=前一个下载通道的完成时间而且父任务的完成时间+传输时长<=当前上传通道的开始时间
                            if (eReadyTime + inEdge.getDataSize() / Channel.getTransferSpeed() <= downloadStartTime) {
                                double channelDownloadStartTime = eReadyTime;
                                double eFinishTime = eReadyTime + inEdge.getDataSize() / Channel.getTransferSpeed();
                                double channelDownloadFinishTime = eFinishTime;
                                //永久分配，最后通过cAllocationListRecord中的记录撤销
                                CAllocation c = new CAllocation(inEdge, parentTask.getPublicVM(),
                                        vl, eReadyTime, eFinishTime, -1, -1,
                                        channelDownloadStartTime, channelDownloadFinishTime, false);
                                //永久分配，最后通过cAllocationListRecord中的记录撤销
                                cAllocationList.add(c);
                                //暂时分配
                                cAllocationListRecord.add(c);

                                //永久分配,这个EAllocation可能以后对画图有帮助，这里暂时记录，其实CAllocation已经包括了,参数VM_Private.NETWORK_SPEED用不到
                                EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPublicVM(),
                                        vl, eReadyTime, VM_Private.NETWORK_SPEED, eFinishTime);
                                this.eAllocationList.add(eAllocation);
                                eAllocationListRecord.add(eAllocation);
                                isFinal = false;
                                DAT = Math.max(DAT, eFinishTime);
                                break;
                            }
                        }
                    }

                    //插入在最后
                    if (isFinal == true) {
                        //获取最后一个上传通道的传输完成时间
                        double finalFinishTime;
                        if(downloadFinishTimeList.size() == 0){    //不加这个判断会报错
                            finalFinishTime = 0;
                        }
                        else{
                            finalFinishTime = downloadFinishTimeList.get(downloadFinishTimeList.size()-1);
                        }
                        double eReadyTime = Math.max(finalFinishTime, parentTask.getAFT());
                        double channelDownloadStartTime = eReadyTime;
                        double eFinishTime = eReadyTime + inEdge.getDataSize() / Channel.getTransferSpeed();
                        double channelDownloadFinishTime = eFinishTime;
                        //永久分配，最后通过cAllocationListRecord中的记录撤销
                        CAllocation c = new CAllocation(inEdge, parentTask.getPublicVM(),
                                vl, eReadyTime, eFinishTime, -1, -1,
                                channelDownloadStartTime, channelDownloadFinishTime, false);
                        //永久分配，最后通过cAllocationListRecord中的记录撤销
                        cAllocationList.add(c);
                        //暂时分配
                        cAllocationListRecord.add(c);

                        //永久分配,这个EAllocation可能以后对画图有帮助，这里暂时记录，其实CAllocation已经包括了,参数VM_Private.NETWORK_SPEED用不到
                        EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPublicVM(),
                                vl, eReadyTime, VM_Private.NETWORK_SPEED, eFinishTime);
                        this.eAllocationList.add(eAllocation);
                        eAllocationListRecord.add(eAllocation);
                        DAT = Math.max(DAT, eFinishTime);
                    }

                }

            }
        }
        //进行回滚操作，true回滚
        if (step == 1) {
            for (CAllocation cAllocation : cAllocationListRecord) {

                this.cAllocationList.remove(cAllocation);

            }
            for (EAllocation eAllocation : eAllocationListRecord) {
                this.eAllocationList.remove(eAllocation);
            }
        }

        return DAT;
    }


    //distributionForecastTable,only used in CCSH
    private Double dFT(Task task,VM_Private privateVM){
        Double result=0.0;
        if(task.getName().equals("exit")) return result;
        for(Edge outEdge: task.getOutEdges()){
            Task succTask = outEdge.getDestination();

            //判断是否是私密任务，是私密任务就是云内传输
            if(succTask.getRunOnPrivateOrPublic() == true){
                result+= succTask.getTaskSize()/VM_Private.SPEEDS[VM_Private.SLOWEST];
                result+= outEdge.getDataSize()/VM.NETWORK_SPEED;
            }
            else{

                result+=succTask.getTaskSize()/VM_Public.SPEEDS[VM_Public.FASTEST];
                result+=outEdge.getDataSize()/Channel.getTransferSpeed();
            }

        }
        return result;
    }

    private Double dFT(Task task,VM_Public publicVM){
        Double result=0.0;
        for(Edge outEdge: task.getOutEdges()) {
            Task succTask = outEdge.getDestination();

            //判断是否是私密任务，是私密任务就是云内传输
            if (succTask.getRunOnPrivateOrPublic() == true) {
                result += succTask.getTaskSize() / VM_Private.SPEEDS[VM_Private.SLOWEST];
                result += outEdge.getDataSize() / Channel.getTransferSpeed();
            } else {

                result += succTask.getTaskSize() / VM_Public.SPEEDS[VM_Public.FASTEST];
                result += outEdge.getDataSize() / VM.NETWORK_SPEED;
            }
        }
        return result;
    }

    //Algorithm 2 ALLocateIncomingEdges(n$_j$,v$_l$),公有云上
    public double allocateIncomingEdges(Task nj, VM_Public vl, int step) {
        /*
        和上面那个函数一样，只是形参的类型不同，多态。
         */
        List<Edge> inEdges = new ArrayList<Edge>(nj.getInEdges());

        //line 1
        Collections.sort(inEdges, new Comparator<Edge>() {
            @Override
            public int compare(Edge e1, Edge e2) {
                if (e1.getSource().getTaskSize() == 0 && e1.getSource().getInEdges() == null)  //entry
                    return 1;
                if (e2.getSource().getTaskSize() == 0 && e2.getSource().getInEdges() == null)  //entry
                    return -1;

                double eReadyTime1 = getMinEReadyTime(e1, vl);    //ready time
                double eReadyTime2 = getMinEReadyTime(e2, vl);
                //论文中算法2里的ts(ei,j, μ(ni));的计算就是和这里一样的，也是暗含插入
                return Double.compare(eReadyTime1, eReadyTime2);
            }
        });//按照源的DAT升序排序InEdges；	parentTask在该vm上的edge，排在哪里都无所谓

        //Line2
        double DAT = 0;

        //记录临时的分配边和分配的通道
        ArrayList<EAllocation> eAllocationListRecord = new ArrayList<>();
        ArrayList<CAllocation> cAllocationListRecord = new ArrayList<>();

        //Line3
        for (Edge inEdge : inEdges) {


            Task parentTask = inEdge.getSource();

            //entryTask
            if (parentTask.getTaskSize() == 0
                    && (parentTask.getInEdges() ==null
                    ||parentTask.getInEdges().size() == 0))  //entry
                continue;

            //exitTask，这里不太对，但不影响结果
            if (parentTask.getTaskSize() == 0 && parentTask.getOutEdges() == null)
                continue;

            //（1）Line4
            //判断是getPrivateVM还是getPublicVM
            /*
            if(parentTask.getRunOnPrivateOrPublic() == true){
                continue;
            }

             */

            if ((parentTask.getRunOnPrivateOrPublic() == false) && parentTask.getPublicVM() == vl) {

                DAT = Math.max(DAT, parentTask.getAFT());  //line5同一个处理器上
                double eReadyTime = parentTask.getAFT();
                double eFinishTime = eReadyTime ;
                EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPublicVM(), vl, eReadyTime, VM_Public.NETWORK_SPEED, eFinishTime);
                this.eAllocationList.add(eAllocation);
                eAllocationListRecord.add(eAllocation);
            }


            //(2)Line6
            else if ((parentTask.getPublicVM() != vl)
                    && (parentTask.getRunOnPrivateOrPublic() == false)) //前面是判断u_i ！= v_l，后面一个相当于异或
            {
                //Line7
                double eReadyTime = parentTask.getAFT();
                double eFinishTime = eReadyTime + inEdge.getDataSize() / VM_Public.NETWORK_SPEED;


                //line8
                EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPublicVM(), vl, eReadyTime, VM_Public.NETWORK_SPEED, eFinishTime);
                this.eAllocationList.add(eAllocation);
                eAllocationListRecord.add(eAllocation);

                //Line9
                DAT = Math.max(DAT, eFinishTime);
            }


            //(3)Line10
            else {
                //因为vl已经是公有云上服务器，所以只需要判断ui是否是私有云
                if (parentTask.getRunOnPrivateOrPublic() == true) {  //line11
                    //double eReadyTime = edgeReadyTimeFromPrivateTOPublic(inEdge);
                    //获取当前上传通道的每个Edge的开始传输时间和结束传输时间。理论上需要
                    //通过CAllocation获取
                    ArrayList<Double> uploadStartTimeList = new ArrayList<>();
                    ArrayList<Double> uploadFinishTimeList = new ArrayList<>();
                    for (CAllocation cAllocation : cAllocationList) {
                        if (cAllocation.getFlag() == true) {  //true就是代表从私有云到公有云
                            uploadStartTimeList.add(cAllocation.getChannelUploadStartTime());
                            uploadFinishTimeList.add(cAllocation.getChannelUploadFinishTime());
                        }
                    }
                    Collections.sort(uploadStartTimeList);   //从小到大排序
                    Collections.sort(uploadFinishTimeList);


                    boolean isFinal = true;//如果为true就说明到最后也插入不进去了，直接放在
                    //逻辑是插入在当前上传任务的完成时间和下一个上传任务的开始时间之间

                    for (int i = 0; i < uploadStartTimeList.size(); i++) {
                        double uploadStartTime = uploadStartTimeList.get(i);

                        //这个判断是为了判断如果输入边的父任务在下一个上传阶段开始时间的时候还没有完成，则可以直接跳过此阶段
                        if (parentTask.getAFT() >= uploadStartTimeList.get(i)) {
                            continue;
                        }

                        //插入在开头，即所有上传边之前
                        if (i == 0) {

                            if (parentTask.getAFT() + inEdge.getDataSize() / Channel.getTransferSpeed() <= uploadStartTime) {
                                //此时边准备好了就可以传入了
                                double eReadyTime = parentTask.getAFT();
                                double eFinishTime = eReadyTime + inEdge.getDataSize() / Channel.getTransferSpeed();
                                double channelUploadStartTime = eReadyTime;
                                double channelUploadFinishTime = eFinishTime;

                                CAllocation c = new CAllocation(inEdge, parentTask.getPriavteVM(),
                                        vl,  eReadyTime, eFinishTime, channelUploadStartTime, channelUploadFinishTime,
                                        -1, -1, true);
                                //永久分配，最后通过cAllocationListRecord中的记录撤销
                                cAllocationList.add(c);
                                //暂时分配
                                cAllocationListRecord.add(c);

                                //永久分配,这个EAllocation可能以后对画图有帮助，这里暂时记录，其实CAllocation已经包括了
                                EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPriavteVM(), vl, eReadyTime, Channel.getTransferSpeed(), eFinishTime);
                                this.eAllocationList.add(eAllocation);

                                eAllocationListRecord.add(eAllocation);
                                isFinal = false;
                                DAT = Math.max(DAT, eFinishTime);
                                break;
                            }

                        } else {
                            //前一个任务的完成时间，这里i永远不会为0，所以不会出错
                            double uploadPreviousFinishTime = uploadFinishTimeList.get(i - 1);
                            double eReadyTime = Math.max(uploadPreviousFinishTime, parentTask.getAFT());
                            //父任务的完成时间>=上传通道的完成时间而且父任务的完成时间+传输时长<=下一个上传通道的开始时间
                            if (eReadyTime + inEdge.getDataSize() / Channel.getTransferSpeed() <= uploadStartTime) {

                                double channelUploadStartTime = eReadyTime;
                                double eFinishTime = eReadyTime + inEdge.getDataSize() / Channel.getTransferSpeed();
                                double channelUploadFinishTime = eFinishTime;
                                //永久分配，最后通过cAllocationListRecord中的记录撤销
                                CAllocation c = new CAllocation(inEdge, parentTask.getPriavteVM(),
                                        vl,  eReadyTime, eFinishTime, channelUploadStartTime, channelUploadFinishTime,
                                        -1, -1, true);
                                //永久分配，最后通过cAllocationListRecord中的记录撤销
                                cAllocationList.add(c);
                                //暂时分配
                                cAllocationListRecord.add(c);

                                //永久分配,这个EAllocation可能以后对画图有帮助，这里暂时记录，其实CAllocation已经包括了
                                EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPriavteVM(), vl, eReadyTime, Channel.getTransferSpeed(), eFinishTime);
                                this.eAllocationList.add(eAllocation);

                                eAllocationListRecord.add(eAllocation);
                                isFinal = false;
                                DAT = Math.max(DAT, eFinishTime);
                                break;
                            }
                        }
                    }

                    //插入在最后
                    if (isFinal == true) {
                        //获取最后一个上传通道的传输完成时间
                        double finalFinishTime;
                        if(uploadFinishTimeList.size() == 0){    //不加这个判断吗会报错
                            finalFinishTime = 0;
                        }
                        else{
                            finalFinishTime = uploadFinishTimeList.get(uploadFinishTimeList.size()-1);
                        }

                        double eReadyTime = Math.max(finalFinishTime, parentTask.getAFT());
                        double channelUploadStartTime = eReadyTime;
                        double eFinishTime = eReadyTime + inEdge.getDataSize() / Channel.getTransferSpeed();
                        double channelUploadFinishTime = eFinishTime;
                        //永久分配，最后通过cAllocationListRecord中的记录撤销
                        CAllocation c = new CAllocation(inEdge, parentTask.getPriavteVM(),
                                vl, eReadyTime, eFinishTime, channelUploadStartTime, channelUploadFinishTime,
                                -1, -1, true);
                        //永久分配，最后通过cAllocationListRecord中的记录撤销
                        cAllocationList.add(c);
                        //暂时分配
                        cAllocationListRecord.add(c);

                        //永久分配,这个EAllocation可能以后对画图有帮助，这里暂时记录，其实CAllocation已经包括了
                        EAllocation eAllocation = new EAllocation(inEdge, parentTask.getPriavteVM(), vl, eReadyTime, Channel.getTransferSpeed(), eFinishTime);
                        this.eAllocationList.add(eAllocation);

                        eAllocationListRecord.add(eAllocation);

                        DAT = Math.max(DAT, eFinishTime);
                    }
                }
            }
        }

        //进行回滚操作，true回滚
        if (step == 1) {
            for (CAllocation cAllocation : cAllocationListRecord) {
                this.cAllocationList.remove(cAllocation);
            }
            for (EAllocation eAllocation : eAllocationListRecord) {
                this.eAllocationList.remove(eAllocation);
            }
        }

        return DAT;
    }


    //其实edgeReadyTimeTransfer = taskFinishTime
    private double getMinEReadyTime(Edge inEdge, VM vm) {
        Task parentTask = inEdge.getSource();
        double minEReadyTime = parentTask.getAFT();
        return minEReadyTime;
    }


    private double edgeReadyTimeFromPrivateTOPublic(Edge inEdge, VM_Private vi) {
        return 1;
    }


    //ture:私有云;false:公有云
    public static <T extends VM> ArrayList<T> getCandidate(ArrayList<T> vmList, int type, boolean flag) {
        boolean emptyFlag = false;
        for (VM vm : vmList) {
            if (vm.getTasks() == null||vm.getTasks().size() == 0) {
                emptyFlag = true;
                break;
            }
        }

        if (vmList.size() == 0 || emptyFlag == false) {
            if (flag == true) {
                VM vm = new VM_Private(++(VM_Private.idInterval), type);
                vmList.add((T) vm);
            } else {
                VM vm = new VM_Public(++(VM_Public.idInterval), type);
                vmList.add((T) vm);
            }
        }

        Collections.sort(vmList, new Comparator<VM>() {
            @Override
            public int compare(VM v1, VM v2) {
                if (  v1.getTasks() == null || v1.getTasks().size() == 0) {
                    return 1;
                }
                if (  v2.getTasks() == null || v2.getTasks().size() == 0) {
                    return -1;
                }
                return 0;
            }
        });  //空的vm放后面
        return vmList;
    }
}



