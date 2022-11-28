package contentionAware;

import contentionFree.Solution;
import contentionFree.TAllocation;
import setting.*;

import java.util.*;

public class ChannelSolution {

    // The class Solution stores task scheduling information in mapping and revMapping.
    // It stores all edge allocations on the incoming bandwidth of each VM
    //它在每个VM的传入带宽上存储所有边缘分配
    private HashMap<VM, List<contentionAware.EAllocation>> eaInMap = new HashMap<VM, List<contentionAware.EAllocation>>();
    // It stores all edge allocations on the outgoing bandwidth of each VM
    private HashMap<VM, List<contentionAware.EAllocation>> eaOutMap = new HashMap<VM, List<contentionAware.EAllocation>>();

    //TAllocation List is sorted based on startTime
    //因为一个task可能被复制，所以一个task在一个solution中可有多个allocation
    protected HashMap<VM, List<TAllocation>> mapping = new HashMap<>();

    //reverseMapping: the content in revMapping is the same as that in mapping
    //used to make get_Allocation_by_Task easy
    protected HashMap<Task, List<TAllocation>> revMapping = new HashMap<>();


    public void seteAllocations(ArrayList<EAllocation> eAllocations) {
        this.eAllocations = eAllocations;
    }

    public void setcAllocations(ArrayList<CAllocation> cAllocations) {
        this.cAllocations = cAllocations;
    }

    public void settAllocations(ArrayList<TAllocation> tAllocations) {
        this.tAllocations = tAllocations;
    }

    ArrayList<EAllocation> eAllocations = new ArrayList<EAllocation>();
    ArrayList<CAllocation> cAllocations = new ArrayList<CAllocation>();
    ArrayList<TAllocation> tAllocations = new ArrayList<TAllocation>();

    public ChannelSolution() {
    }

    ;

    public ChannelSolution(ArrayList<EAllocation> eAllocations, ArrayList<CAllocation> cAllocations, ArrayList<TAllocation> tAllocations) {
        this.eAllocations = eAllocations;
        this.cAllocations = cAllocations;
        this.tAllocations = tAllocations;
    }

    public ArrayList<EAllocation> geteAllocations() {
        return eAllocations;
    }

    public void addEdge(EAllocation ea) {
        addEdgeOneSide(eaOutMap, ea, ea.getSourceVM());
        addEdgeOneSide(eaInMap, ea, ea.getDestVM());

    }

    private void addEdgeOneSide(HashMap<VM, List<contentionAware.EAllocation>> eMap, contentionAware.EAllocation ea, VM vm) {
        if (eMap.get(vm) == null)
            eMap.put(vm, new ArrayList<contentionAware.EAllocation>());

        List<contentionAware.EAllocation> eaList = eMap.get(vm);
        for (int i = 0; i <= eaList.size(); i++) {
            if (i == eaList.size()) {
                eaList.add(i, ea);
                break;        //这个break是必须加的，否则<=的条件一直成立的
            } else if (ea.getStartTime() < eaList.get(i).getStartTime()    // ||后的条件是针对执行时间为0的边
                    || (ea.getStartTime() == eaList.get(i).getStartTime() &&
                    ea.getFinishTime() < eaList.get(i).getFinishTime())) {
                //因为Adaptor中数据边传输是可以overlap的，所以把以下检测取消掉
//				if(Config.isDebug() && ea.getFinishTime() > eaList.get(i).getStartTime()) {
//					System.out.println(ea);
//					throw new RuntimeException("Critical Error: EAllocation conflicts");
//				}
                eaList.add(i, ea);
                break;
            }
        }
    }

    public double getVMFinishTime(VM vm) {
        if (mapping.get(vm) == null || mapping.get(vm).size() == 0)
            return VM.LAUNCH_TIME;
        else {
            List<TAllocation> allocations = mapping.get(vm);
            return allocations.get(allocations.size() - 1).getFinishTime();
        }
    }

    public ArrayList<CAllocation> getcAllocations() {
        return cAllocations;
    }

    public ArrayList<TAllocation> gettAllocations() {
        return tAllocations;
    }

    public TAllocation getFirstTA(Task task) {
        TAllocation selectedTAllocation = null;
        for (TAllocation tAllocation : tAllocations) {
            if (tAllocation.getTask() == task) {
                selectedTAllocation = tAllocation;
                break;
            }
        }
        return selectedTAllocation;
    }

    //------------------------------add / remove a task allocation----------------------------
    // index = -1表示直接插在结尾;  返回的allocation是为了方便撤销插入
    private TAllocation addTaskToVMWithIndex(VM vm, Task task, double startTime, int index) {
        if (mapping.containsKey(vm) == false)
            mapping.put(vm, new LinkedList<TAllocation>());
        TAllocation alloc = new TAllocation(vm, task, startTime);
        if (Config.isDebug()) {
            List<TAllocation> list = mapping.get(vm);
            double startTime1 = alloc.getStartTime();
            double finishTime1 = alloc.getFinishTime();
            boolean conflict = false;                //check whether there is time conflict
            for (TAllocation prevAlloc : list) {
                double startTime2 = prevAlloc.getStartTime();
                double finishTime2 = prevAlloc.getFinishTime();
                if ((startTime1 > startTime2 && startTime1 < finishTime2)      //startTime2 is between startTime1 and finishTime1
                        || (startTime2 > startTime1 && finishTime1 > startTime2)) //startTime1 is between startTime2 and finishTime2
                    conflict = true;        //这里的判断条件应该有错误，剑钊反映的。虽然不影响整体逻辑。
            }
            if (conflict)
                throw new RuntimeException("Critical Error: TAllocation conflicts");
        }
        if (index == -1)
            mapping.get(vm).add(alloc);
        else
            mapping.get(vm).add(index, alloc);
        if (revMapping.get(task) == null)
            revMapping.put(task, new ArrayList<TAllocation>());
        revMapping.get(task).add(alloc);
        return alloc;

    }

    //寻找指定位置进行插入，分为三种情况：1.vm是空的；2.vm里能插入；3.若不能插入则在最后
    public TAllocation addTaskToVM(VM vm, Task task, double startTime) {
        if (mapping.containsKey(vm) == false)
            return addTaskToVMWithIndex(vm, task, startTime, -1);

        List<TAllocation> allocations = mapping.get(vm);
        for (int i = 0; i < allocations.size(); i++) {        //插入
            TAllocation curAlloc = allocations.get(i);
            if (startTime < curAlloc.getStartTime() ||
                    (startTime == curAlloc.getStartTime() && task.getTaskSize() == 0)) {
                //第二个条件是因为如entry等大小为0的任务的存在，必须把他们这么放curAlloc之前
                return addTaskToVMWithIndex(vm, task, startTime, i);
            }
        }
        return addTaskToVMWithIndex(vm, task, startTime, -1);    //最后

    }

    public TAllocation addTaskToVMEnd(VM vm, Task task, double startTime) {
        return addTaskToVMWithIndex(vm, task, startTime, -1);
    }

    public double getMakespan() {        //本质上就是exit-entry时间
        double makespan;
        makespan = this.gettAllocations().get(this.gettAllocations().size() - 1).getFinishTime() -
                this.gettAllocations().get(0).getFinishTime();
        return makespan;
    }


    public List<TAllocation> getTAList(Task t) {
        List<TAllocation> taList = revMapping.get(t);
        return taList == null ? null : Collections.unmodifiableList(taList);
    }

}
