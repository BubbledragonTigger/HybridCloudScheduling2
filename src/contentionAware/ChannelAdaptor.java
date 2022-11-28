package contentionAware;


import java.util.*;

import contentionFree.TAllocation;
import contentionAware.CAllocation;
import setting.*;
public class ChannelAdaptor {
    private HashMap<Channel,List<CASnapshot>> uploadMap = new HashMap<>();
    private HashMap<Channel,List<CASnapshot>> downloadMap = new HashMap<>();
    private HashMap<Edge,List<CASnapshot>> edgeMap = new HashMap<>();

    private Channel uploadChannel = new Channel("uploadChannel");
    private Channel downloadChannel = new Channel("downloadChannel");
    public static enum UpOrDown{UP,DOWN,NULL};

    /**
     * reschedule
     * @param channelSolution
     * @param wf
     * @return
     */
    public ChannelSolution buildFromSolutionExclusive(ChannelSolution channelSolution, Workflow wf,TProperties.Type type){
        //将PrivateVM和PublicVM转成 VM；
        for(TAllocation tAllocation: channelSolution.gettAllocations()){
            if(tAllocation.getPrivateVM() != null)
                tAllocation.setVM(tAllocation.getPrivateVM()) ;
            else
                tAllocation.setVM(tAllocation.getPublicVM());
        }

        ChannelSolution newChannelSolution = new ChannelSolution();
        HashMap<Task , Integer> inEdgeCounts = new HashMap<Task, Integer>();
        final HashMap<Task, Double> props = new TProperties(wf,type);

        //waitingEdges: whose source tasks have finished and waiting for allocating bandwidth resources

        TreeSet<CASnapshot> waitingEdgesUploadSnaps = new TreeSet<>((CASnapshot a, CASnapshot b) ->{
            double a1 = props.get(a.getEdge().getDestination());
            double b1 = props.get(b.getEdge().getDestination());
            //double a1 =- a.getEdge().getDataSize();
            //double b1 = -b.getEdge().getDataSize();
            int i = Double.compare(a1, b1);
            return i!=0 ? -1*i : 1;		// equality is not allowed, 否则会出现覆盖
        });
        TreeSet<CASnapshot> waitingEdgesDownloadSnaps = new TreeSet<>((CASnapshot a, CASnapshot b) ->{
//            double a1 = props.get(a.getEdge().getDestination());
//            double b1 = props.get(b.getEdge().getDestination());
            double a1 = -a.getEdge().getDataSize();
            double b1 = -b.getEdge().getDataSize();
            int i = Double.compare(a1, b1);
            return i!=0 ? -1*i : 1;		// equality is not allowed, 否则会出现覆盖
        });
        TreeSet<CASnapshot> waitingEdges = new TreeSet<>((CASnapshot a, CASnapshot b) ->{
            double a1 = props.get(a.getEdge().getDestination());
            double b1 = props.get(b.getEdge().getDestination());
            int i = Double.compare(a1, b1);
            return i!=0 ? -1*i : 1;		// equality is not allowed, 否则会出现覆盖
        });
        TreeSet<CASnapshot> waitingAllEdges = new TreeSet<>((CASnapshot a, CASnapshot b) ->{
            double a1 = props.get(a.getEdge().getDestination());
            double b1 = props.get(b.getEdge().getDestination());
            int i = Double.compare(a1, b1);
            return i!=0 ? -1*i : 1;		// equality is not allowed, 否则会出现覆盖
        });



        List<CASnapshot> ingEdgeUploadSnaps = new ArrayList<>();  //记录通过上传通道的边
        List<CASnapshot> ingEdgeDownloadSnaps = new ArrayList<>();  //记录通过上传通道的边
        List<CASnapshot> ingEdgeSnaps = new ArrayList<>();  //不包含ingEdgeUploadSnaps和ingEdgeDownloadSnaps
        List<CASnapshot> ingallEdgeSnaps = new ArrayList<>();


        PriorityQueue<TAllocation> ingTasks = new PriorityQueue<>((TAllocation a, TAllocation b) -> {
            return Double.compare(a.getFinishTime(), b.getFinishTime());
        });
        TAllocation allocEntry = channelSolution.getFirstTA(wf.getEntryTask());
        ingTasks.add(allocEntry);

        copyTAllocation(allocEntry,0,newChannelSolution);

        double currentTime = 0;//the current time during simulating execution via tc
        //Each one in ingTasks and ingEdgeSnaps is in execution
        //任务的结束时间是直接确定的，边的结束时间要动态确定——未确定的时候设为最大值
        //In each iteration, an edge or task finishes execution, and updates currentTime to its finish time
        while(ingTasks.size()>0 || ingallEdgeSnaps.size() > 0){ //line3
            CASnapshot curES = null;
            for(CASnapshot tempES : ingallEdgeSnaps){
                if(curES == null || tempES.getRequiredTime() < curES.getRequiredTime()){
                    curES = tempES;
                }
            }
            TAllocation curTAlloc = ingTasks.peek();
            recordUpOrDownChannel(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                    ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),currentTime,4);
            if(curTAlloc == null || curES!= null && curES.getRequiredTime() < curTAlloc.getFinishTime()-currentTime){
                //line5
                ingallEdgeSnaps.remove(curES);
                Edge curEdge = curES.getEdge();
                double timeDiff = curES.getRequiredTime();

                if(!(ingEdgeUploadSnaps.contains(curES) || ingEdgeDownloadSnaps.contains(curES))){

                    ingEdgeSnaps.remove(curES);


                    for(CASnapshot eaSnapshot: ingEdgeSnaps) {
                        eaSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeUploadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }

                    //add to newChannelSolution
                    EAllocation ealloc = new EAllocation(curEdge,curES.getsVM(),curES.getdVM()  //因为有finishTIme,速度无需计算
                            ,currentTime,-1,currentTime + timeDiff);
                    copyEAllocation(ealloc,newChannelSolution,UpOrDown.NULL);

                    //re-determine bandwidth for ingEdgeSnaps
                    determinePortionReschedule(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                            ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),UpOrDown.NULL,timeDiff,currentTime);
                }
                else if(ingEdgeUploadSnaps.contains(curES)){

                    ingEdgeUploadSnaps.remove(curES);
                    for(CASnapshot eaSnapshot: ingEdgeSnaps) {
                        eaSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeUploadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }

                    //add to newChannelSolution
                    EAllocation ealloc = new EAllocation(curEdge,curES.getsVM(),curES.getdVM()  //因为有finishTIme,速度无需计算,这里直接写成-1
                            ,currentTime,-1,currentTime + timeDiff);
                    copyEAllocation(ealloc,newChannelSolution,UpOrDown.UP);
                    //re-determine bandwidth for ingEdgeSnaps
                    determinePortionReschedule(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                            ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),UpOrDown.UP,timeDiff,currentTime);
                }
                else{
                    ingEdgeDownloadSnaps.remove(curES);
                    //add to newChannelSolution

                    for(CASnapshot eaSnapshot: ingEdgeSnaps) {
                        eaSnapshot.dataSubstract(timeDiff);
                    }

                    for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeUploadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }

                    EAllocation ealloc = new EAllocation(curEdge,curES.getsVM(),curES.getdVM()  //因为有finishTIme,速度无需计算,这里直接写成-1
                            ,currentTime,-1,currentTime + timeDiff);
                    copyEAllocation(ealloc,newChannelSolution,UpOrDown.DOWN);
                    //re-determine bandwidth for ingEdgeSnaps
                    determinePortionReschedule(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                            ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),UpOrDown.DOWN,timeDiff,currentTime);
                }
                currentTime = currentTime + timeDiff;
                Task dTask = curEdge.getDestination();
                Integer count = inEdgeCounts.get(dTask);
                inEdgeCounts.put(dTask, count != null ? count+1 : 1);
                if(inEdgeCounts.get(dTask) == dTask.getInEdges().size()) {//all inEdges of dTask have finished
                    VM dVM = channelSolution.getFirstTA(dTask).getVM();
                    double startTime = Math.max(currentTime, channelSolution.getVMFinishTime(dVM));
                    TAllocation alloc = channelSolution.addTaskToVMEnd(dVM,dTask,startTime);
                    ingTasks.add(alloc);
                    newChannelSolution.gettAllocations().add(alloc);
                }
            }
            else{
                ingTasks.poll();
                double timeDiff = curTAlloc.getFinishTime() -currentTime;

                currentTime = currentTime + timeDiff;

                for(CASnapshot eaSnapshot: ingEdgeSnaps) {
                    eaSnapshot.dataSubstract(timeDiff);
                }
                for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
                    caSnapshot.dataSubstract(timeDiff);
                }
                for(CASnapshot caSnapshot : ingEdgeUploadSnaps){
                    caSnapshot.dataSubstract(timeDiff);
                }

                for(Edge outEdge : curTAlloc.getTask().getOutEdges()){
                    Task dTask = outEdge.getDestination();
                    TAllocation dAlloc = channelSolution.getFirstTA(dTask);
                    double dataSize = curTAlloc.getVM() == dAlloc.getVM() ? 0 : outEdge.getDataSize();
                    CASnapshot snap = new CASnapshot(outEdge, curTAlloc.getVM(), dAlloc.getVM(),
                            currentTime, dataSize);

                    if((outEdge.getSource().getRunOnPrivateOrPublic() == true && outEdge.getDestination().getRunOnPrivateOrPublic() == true)
                            || (outEdge.getSource().getRunOnPrivateOrPublic() == false && outEdge.getDestination().getRunOnPrivateOrPublic() == false))
                        //ingEdgeSnaps.add(snap);
                        ingEdgeSnaps.add(snap);
                    else if(outEdge.getSource().getRunOnPrivateOrPublic() == true && outEdge.getDestination().getRunOnPrivateOrPublic() == false)
                        waitingEdgesUploadSnaps.add(snap);
                    else
                        waitingEdgesDownloadSnaps.add(snap);
                }
                determinePortionReschedule(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                        ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),UpOrDown.NULL,timeDiff,currentTime);  //所有的边的数量和之前一样，所以直接设置为null就可以

                //update remDataSize for ingEdgeSnaps
            }
            ingallEdgeSnaps.clear();
            if(ingEdgeUploadSnaps.size()==0){
                if(waitingEdgesUploadSnaps.size()!=0)
                    ingEdgeUploadSnaps.add(waitingEdgesUploadSnaps.pollFirst()) ;
            }
            if(ingEdgeDownloadSnaps.size()==0){
                if(waitingEdgesDownloadSnaps.size()!=0)
                    ingEdgeDownloadSnaps.add(waitingEdgesDownloadSnaps.pollFirst()) ;
            }
            ingallEdgeSnaps.addAll(ingEdgeDownloadSnaps);
            ingallEdgeSnaps.addAll(ingEdgeUploadSnaps);
            ingallEdgeSnaps.addAll(ingEdgeSnaps);

        }

        System.out.println(" adaptorReschedule finished");
        return newChannelSolution;

    }

    /**
     * 共享式
     * @param channelSolution
     * @param wf
     * @return
     */
    public ChannelSolution buildFromSolutionShared(ChannelSolution channelSolution, Workflow wf){
        //将PrivateVM和PublicVM转成 VM；
        for(TAllocation tAllocation: channelSolution.gettAllocations()){
            if(tAllocation.getPrivateVM() != null)
                tAllocation.setVM(tAllocation.getPrivateVM()) ;
            else
                tAllocation.setVM(tAllocation.getPublicVM());
        }


        ChannelSolution newChannelSolution = new ChannelSolution();
        HashMap<Task , Integer> inEdgeCounts = new HashMap<Task, Integer>();

        List<CASnapshot> ingEdgeUploadSnaps = new ArrayList<>();  //记录通过上传通道的边
        List<CASnapshot> ingEdgeDownloadSnaps = new ArrayList<>();  //记录通过上传通道的边
        List<CASnapshot> ingEdgeSnaps = new ArrayList<>();  //不包含ingEdgeUploadSnaps和ingEdgeDownloadSnaps
        List<CASnapshot> ingallEdgeSnaps = new ArrayList<>();

        PriorityQueue<TAllocation> ingTasks = new PriorityQueue<>((TAllocation a, TAllocation b) -> {
            return Double.compare(a.getFinishTime(), b.getFinishTime());
        }); //正在传输的任务

        TAllocation allocEntry = channelSolution.getFirstTA(wf.getEntryTask());
        ingTasks.add(allocEntry);
        copyTAllocation(allocEntry,0,newChannelSolution);

        double currentTime = 0;//the current time during simulating execution via tc
        //Each one in ingTasks and ingEdgeSnaps is in execution
        //任务的结束时间是直接确定的，边的结束时间要动态确定——未确定的时候设为最大值
        //In each iteration, an edge or task finishes execution, and updates currentTime to its finish time

        while(ingTasks.size()>0 || ingallEdgeSnaps.size() > 0){
            //获取最小EASnapshot,i.e.就是最先完成的边
            CASnapshot curES = null;
            for(CASnapshot tempES : ingallEdgeSnaps){
                if(curES == null || tempES.getRequiredTime() < curES.getRequiredTime()){
                    curES = tempES;
                }
            }

            TAllocation curTAlloc = ingTasks.peek();
            recordUpOrDownChannel(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                    ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),currentTime,4);
            //edgeSnap or task?
            if(curTAlloc == null || curES!= null && curES.getRequiredTime() < curTAlloc.getFinishTime()-currentTime){
                //line5
                ingallEdgeSnaps.remove(curES);
                Edge curEdge = curES.getEdge();
                double timeDiff = curES.getRequiredTime();

                if(!(ingEdgeUploadSnaps.contains(curES) || ingEdgeDownloadSnaps.contains(curES))){

                    ingEdgeSnaps.remove(curES);


                    for(CASnapshot eaSnapshot: ingEdgeSnaps) {
                        eaSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeUploadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }

                    //add to newChannelSolution
                    EAllocation ealloc = new EAllocation(curEdge,curES.getsVM(),curES.getdVM()  //因为有finishTIme,速度无需计算
                                ,curES.getStartTime(),-1,currentTime + timeDiff);
                    copyEAllocation(ealloc,newChannelSolution,UpOrDown.NULL);

                    //re-determine bandwidth for ingEdgeSnaps
                    determinePortion(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                        ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),UpOrDown.NULL,timeDiff,currentTime);
                }
                else if(ingEdgeUploadSnaps.contains(curES)){

                    ingEdgeUploadSnaps.remove(curES);
                    for(CASnapshot eaSnapshot: ingEdgeSnaps) {
                        eaSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeUploadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }

                    //add to newChannelSolution
                    EAllocation ealloc = new EAllocation(curEdge,curES.getsVM(),curES.getdVM()  //因为有finishTIme,速度无需计算,这里直接写成-1
                            ,curES.getStartTime(),-1,currentTime + timeDiff);
                    copyEAllocation(ealloc,newChannelSolution,UpOrDown.UP);
                    //re-determine bandwidth for ingEdgeSnaps
                    determinePortion(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                            ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),UpOrDown.UP,timeDiff,currentTime);
                }
                else{
                    ingEdgeDownloadSnaps.remove(curES);
                    //add to newChannelSolution

                    for(CASnapshot eaSnapshot: ingEdgeSnaps) {
                        eaSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }
                    for(CASnapshot caSnapshot : ingEdgeUploadSnaps){
                        caSnapshot.dataSubstract(timeDiff);
                    }

                    EAllocation ealloc = new EAllocation(curEdge,curES.getsVM(),curES.getdVM()  //因为有finishTIme,速度无需计算,这里直接写成-1
                            ,curES.getStartTime(),-1,currentTime + timeDiff);
                    copyEAllocation(ealloc,newChannelSolution,UpOrDown.DOWN);
                    //re-determine bandwidth for ingEdgeSnaps
                    determinePortion(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                            ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),UpOrDown.DOWN,timeDiff,currentTime);
                }
                currentTime = currentTime + timeDiff;
                Task dTask = curEdge.getDestination();
                Integer count = inEdgeCounts.get(dTask);
                inEdgeCounts.put(dTask, count != null ? count+1 : 1);
                if(inEdgeCounts.get(dTask) == dTask.getInEdges().size()) {//all inEdges of dTask have finished
                    VM dVM = channelSolution.getFirstTA(dTask).getVM();
                    double startTime = Math.max(currentTime, channelSolution.getVMFinishTime(dVM));
                    TAllocation alloc = channelSolution.addTaskToVMEnd(dVM,dTask,startTime);
                    ingTasks.add(alloc);
                    newChannelSolution.gettAllocations().add(alloc);
                }
            }
            else{
                ingTasks.poll();
                double timeDiff = curTAlloc.getFinishTime() -currentTime;

                currentTime = currentTime + timeDiff;

                for(CASnapshot eaSnapshot: ingEdgeSnaps) {
                    eaSnapshot.dataSubstract(timeDiff);
                }
                for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
                    caSnapshot.dataSubstract(timeDiff);
                }
                for(CASnapshot caSnapshot : ingEdgeUploadSnaps){
                    caSnapshot.dataSubstract(timeDiff);
                }

                for(Edge outEdge : curTAlloc.getTask().getOutEdges()){
                    Task dTask = outEdge.getDestination();
                    TAllocation dAlloc = channelSolution.getFirstTA(dTask);
                    double dataSize = curTAlloc.getVM() == dAlloc.getVM() ? 0 : outEdge.getDataSize();
                    CASnapshot snap = new CASnapshot(outEdge, curTAlloc.getVM(), dAlloc.getVM(),
                            currentTime, dataSize);
                    if((outEdge.getSource().getRunOnPrivateOrPublic() == true && outEdge.getDestination().getRunOnPrivateOrPublic() == true)
                     || (outEdge.getSource().getRunOnPrivateOrPublic() == false && outEdge.getDestination().getRunOnPrivateOrPublic() == false))
                        ingEdgeSnaps.add(snap);
                    else if(outEdge.getSource().getRunOnPrivateOrPublic() == true && outEdge.getDestination().getRunOnPrivateOrPublic() == false)
                        ingEdgeUploadSnaps.add(snap);
                    else
                        ingEdgeDownloadSnaps.add(snap);
                }
                determinePortion(new HashSet<CASnapshot>(ingEdgeSnaps),new HashSet<CASnapshot>(ingEdgeUploadSnaps)
                        ,new HashSet<CASnapshot>(ingEdgeDownloadSnaps),UpOrDown.NULL,timeDiff,currentTime);  //所有的边的数量和之前一样，所以直接设置为null就可以

                //update remDataSize for ingEdgeSnaps
            }
            ingallEdgeSnaps.clear();
            ingallEdgeSnaps.addAll(ingEdgeDownloadSnaps);
            ingallEdgeSnaps.addAll(ingEdgeUploadSnaps);
            ingallEdgeSnaps.addAll(ingEdgeSnaps);
        }

        System.out.println(" adaptorShared finished");
        return newChannelSolution;
    }


    private void determinePortion(HashSet<CASnapshot> ingEASnaps,HashSet<CASnapshot>
            ingEdgeUploadSnaps,HashSet<CASnapshot> ingEdgeDownloadSnaps,Enum type,double timeDiff,double currentTime){

        int upSnapNumber = ingEdgeUploadSnaps.size();
        int downSnapNumber = ingEdgeDownloadSnaps.size();
        //if(type == UpOrDown.UP) upSnapNumber = upSnapNumber;  //因为一开始ingEdgeUploadSnaps中去掉了最早执行的边,所以要+1
        //if(type == UpOrDown.DOWN) downSnapNumber = downSnapNumber;
            //对EASnaps,ingEdgeUploadSnaps,ingEdgeDownloadSnaps有不同的处理
            //首先ingEASnaps

        for(CASnapshot eaSnapshot: ingEASnaps){
            eaSnapshot.setBandwidth(VM.NETWORK_SPEED);
        }
        for(CASnapshot caSnapshot: ingEdgeUploadSnaps){
            caSnapshot.setBandwidth(Channel.getTransferSpeed()/ingEdgeUploadSnaps.size());
        }
        for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
            caSnapshot.setBandwidth(Channel.getTransferSpeed() / ingEdgeDownloadSnaps.size());
        }
    }

    private void determinePortionReschedule(HashSet<CASnapshot> ingEASnaps,HashSet<CASnapshot>
            ingEdgeUploadSnaps,HashSet<CASnapshot> ingEdgeDownloadSnaps,Enum type,double timeDiff,double currentTime){

        int upSnapNumber = ingEdgeUploadSnaps.size();
        int downSnapNumber = ingEdgeDownloadSnaps.size();
        //if(type == UpOrDown.UP) upSnapNumber = upSnapNumber;  //因为一开始ingEdgeUploadSnaps中去掉了最早执行的边,所以要+1
        //if(type == UpOrDown.DOWN) downSnapNumber = downSnapNumber;
        //对EASnaps,ingEdgeUploadSnaps,ingEdgeDownloadSnaps有不同的处理
        //首先ingEASnaps

        for(CASnapshot eaSnapshot: ingEASnaps){
            eaSnapshot.setBandwidth(VM.NETWORK_SPEED);
        }
        for(CASnapshot caSnapshot: ingEdgeUploadSnaps){
            caSnapshot.setBandwidth(Channel.getTransferSpeed()/ingEdgeUploadSnaps.size());
        }
        for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
            caSnapshot.setBandwidth(Channel.getTransferSpeed()/ingEdgeDownloadSnaps.size());
        }
    }

    //记录uoLoadMap和downloadMap，方便后续画图
    //type = 1 : edgeMap  2 : donwloadMap  3 : uploadMap  4 All

    private void recordUpOrDownChannel(HashSet<CASnapshot> ingEASnaps,HashSet<CASnapshot>
            ingEdgeUploadSnaps,HashSet<CASnapshot> ingEdgeDownloadSnaps,double currentTime,int type){
        if(type == 1 || type == 4){
            for(CASnapshot eaSnapshot: ingEASnaps){
                if(edgeMap.get(eaSnapshot.getEdge()) == null){
                    edgeMap.put(eaSnapshot.getEdge(),new ArrayList<CASnapshot>());
                }
                if(edgeMap.get(eaSnapshot.getEdge()).contains(eaSnapshot))continue;
                edgeMap.get(eaSnapshot.getEdge()).add(new CASnapshot(eaSnapshot.getEdge(),eaSnapshot.getsVM(),
                        eaSnapshot.getdVM(),currentTime,eaSnapshot.getRemDataSize(),eaSnapshot.getBandwidth()));


            }
        }
       if(type == 2 || type == 4){
           for(CASnapshot caSnapshot : ingEdgeDownloadSnaps){
               if(downloadMap.get(downloadChannel) == null){
                   downloadMap.put(downloadChannel,new ArrayList<CASnapshot>());
               }
               if(downloadMap.get(downloadChannel).contains(caSnapshot))continue;
               downloadMap.get(downloadChannel).add(new CASnapshot(caSnapshot.getEdge(),caSnapshot.getsVM(),
                       caSnapshot.getdVM(),currentTime,caSnapshot.getRemDataSize(),caSnapshot.getBandwidth()));
           }
       }
        if(type == 3 || type == 4){
            for(CASnapshot caSnapshot: ingEdgeUploadSnaps){
                if(uploadMap.get(uploadChannel) == null) {
                    uploadMap.put(uploadChannel, new ArrayList<CASnapshot>());
                }
                if(uploadMap.get(uploadChannel).contains(caSnapshot))continue;
                uploadMap.get(uploadChannel).add(new CASnapshot(caSnapshot.getEdge(),caSnapshot.getsVM(),
                        caSnapshot.getdVM(),currentTime,caSnapshot.getRemDataSize(),caSnapshot.getBandwidth()));
            }
        }
    }
    //拷贝TAllocation，放在newChannelSolution里面，防止和之前独占的相互影响
    private void copyTAllocation(TAllocation tAllocation,double startTime,ChannelSolution channelSolution){

        if(tAllocation.getTask().getRunOnPrivateOrPublic() == true){  //因为之前继承代码写的不好，所以这里只能自己判断VM的类型，true代表在私有云上

            TAllocation newTAllocation = new TAllocation(null,tAllocation.getPrivateVM(),
                    tAllocation.getTask(),startTime,VM_Private.SPEEDS[VM_Private.SLOWEST]);
            channelSolution.gettAllocations().add(newTAllocation);
            channelSolution.addTaskToVM(tAllocation.getPrivateVM(), tAllocation.getTask(),startTime);
        }
        else{

            TAllocation newTAllocation = new TAllocation(tAllocation.getPublicVM(),null,tAllocation.getTask(),
                    startTime,VM_Public.SPEEDS[VM_Public.FASTEST]);
            channelSolution.gettAllocations().add(newTAllocation);
            channelSolution.addTaskToVM(tAllocation.getPublicVM(),tAllocation.getTask(),startTime);
        }



    }


    //深层拷贝EAllocation, 放在newChannelSolution里面，防止和之前独占的相互影响 ifEdgeOrChannel ==1 进行EAllocation和CAllocation添加
    //upOrDown 0为上传通道，1为下降通道,其他不管
    private void copyEAllocation(EAllocation eallo, ChannelSolution channelSolution,Enum upOrDown){

        channelSolution.addEdge(eallo);  //其实没必要深层copy task和edge，因为把信息存放到EAllocation,只要startTime,finishTime进行改变即可
        channelSolution.geteAllocations().add(eallo);
        if(upOrDown == UpOrDown.UP){
                CAllocation cAllocation = new CAllocation(eallo.getEdge(),eallo.getSourceVM(),eallo.getDestVM(),
                        eallo.getStartTime(),eallo.getFinishTime(),eallo.getStartTime(),eallo.getFinishTime(),
                        -1.0,-1.0,true);
                channelSolution.getcAllocations().add(cAllocation);
            }
        else if(upOrDown == UpOrDown.DOWN){
                    CAllocation cAllocation = new CAllocation(eallo.getEdge(),eallo.getSourceVM(),eallo.getDestVM(),
                            eallo.getStartTime(),eallo.getFinishTime(),-1.0,-1.0,
                            eallo.getStartTime(),eallo.getFinishTime(),false);
                    channelSolution.getcAllocations().add(cAllocation);
        }


    }
}



/**
 * A snapshot for channel allocation.
 * Its allocated bandwidth may be different in different time.
 */

class CASnapshot extends EASnapshot{
    private Edge edge;
    private VM sVM,dVM;
    private double startTime;
    private double remDataSize;//remaining data size
    private double bandwidth = 1;//avoid denominator=0 避免分母为0
    public CASnapshot(Edge edge, VM sVM, VM dVM,
                      double startTime , double remDataSize){
        this.edge = edge;
        this.sVM = sVM;
        this.dVM = dVM;
        this.startTime = startTime;
        this. remDataSize = remDataSize;
    }

    public CASnapshot(Edge edge, VM sVM, VM dVM,
                      double startTime , double remDataSize , double bandwidth){
        this.edge = edge;
        this.sVM = sVM;
        this.dVM = dVM;
        this.startTime = startTime;
        this. remDataSize = remDataSize;
        this.bandwidth = bandwidth;
    }

    public CASnapshot(CASnapshot caSnapshot){
        this.edge = caSnapshot.getEdge();
        this.startTime =caSnapshot.getStartTime();
        this.remDataSize = caSnapshot.getRemDataSize();
        this.bandwidth = caSnapshot.getBandwidth();
        this.sVM = caSnapshot.getsVM();
        this.dVM = caSnapshot.getdVM();
    }

    public double getRequiredTime(){
        if(this.bandwidth == 0){	//以上的带宽分配的算法是可能会出现为0的情况的，无法避免
            return Double.MAX_VALUE;
        }
        return this.remDataSize / this.bandwidth;
    }
    private static final double EPS = 0.00001;
    public void dataSubstract(double timeDiff) {
        if(this.remDataSize + EPS < timeDiff * bandwidth)
            throw new RuntimeException("remDataSize is less than 0");
        this.remDataSize -= timeDiff * bandwidth;
        if(this.remDataSize < EPS)
            this.remDataSize = 0;
    }
    public void setBandwidth(double bandwidth) {	this.bandwidth = bandwidth;}

    //-----------------------------------getters-----------------------------
    public Edge getEdge() {	return edge;}
    public VM getsVM() {return sVM;}
    public VM getdVM() {return dVM;}
    public double getStartTime() {return startTime;}
    public double getRemDataSize() {return remDataSize;}
    public double getBandwidth() {	return bandwidth;	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CASnapshot that = (CASnapshot) o;
        return Double.compare(that.startTime, startTime) == 0 && Double.compare(that.remDataSize, remDataSize) == 0 && Double.compare(that.bandwidth, bandwidth) == 0 && Objects.equals(edge, that.edge);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edge, startTime, remDataSize, bandwidth);
    }

    public String toString() {
        return "EdgeSnapshot [" + edge.getSource().getName() + " -> " + edge.getDestination().getName()
                + ", sVM=" + sVM.getId() + ", dVM=" + dVM.getId()
                + ", rem:" + remDataSize +", ban:" + bandwidth+", fTime:"+getRequiredTime()+"]";

    }
}

class CPort{
    private Channel channel;
    private boolean isUp;  //uploading or downloading
    private double remBandWidth;	//remaining bandwidth resource in this port
    private List<CASnapshot> esnaps;
    private long number;  	// # of EASnapshot(!=0) in this port
    public CPort(Channel channel, boolean isUp, double remBandWidth, List<CASnapshot> esnaps) {
        this.channel = channel;
        this.isUp = isUp;
        this.remBandWidth = remBandWidth;
        this.esnaps = esnaps;
        this.number = esnaps.stream()
                .filter(es -> es.getRemDataSize() > 0)
                .count();
    }
    public void bandSubstract(double decrement){
        this.remBandWidth -= decrement;
        this.number--;
        if(this.remBandWidth <0 || this.number <0)
            throw new RuntimeException("remBandWidth is less than 0");
    }
    //--------------------------------------getters-----------------------------
    public Channel getChannel() {	return channel;	}
    public boolean isOut() {	return isUp;	}
    public double getRemBandWidth() {	return remBandWidth;	}
    public List<CASnapshot> getEsnaps() {	return esnaps;}
    public long getNumber() {	return number;	}
    public String toString() {
        return "Port [vm" + channel.getName() + ", " + (isUp ? "out":"in")
                + ", "+number+", " + remBandWidth + "]";
    }
}

