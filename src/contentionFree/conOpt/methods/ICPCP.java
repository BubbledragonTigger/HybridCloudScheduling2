package contentionFree.conOpt.methods;

import contentionFree.Solution;
import contentionFree.TAllocation;
import setting.Edge;
import setting.Task;
import setting.VM;
import setting.Workflow;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Abrishami, Saeid, Mahmoud Naghibzadeh, and Dick HJ Epema. "Deadline-constrained workflow scheduling algorithms 
 * for Infrastructure as a Service Clouds." Future Generation Computer Systems 29.1 (2013): 158-169.
 */
/*
public class ICPCP implements Scheduler {
	
	private final double bestVMSpeed = VM.SPEEDS[VM.FASTEST];
	private Workflow wf;
	private double deadline;
	private ArrayList<TaskWrapper> workflow;  // the workflow used in this algorithm
	private Solution solution ;
	
	public Solution schedule(Workflow wf, double deadline) {
		this.wf = wf;
		this.deadline = deadline;
		this.workflow = new ArrayList<TaskWrapper>();
		for(Task t:wf.getTaskList())
			this.workflow.add(new TaskWrapper(t));
		
		this.solution = new Solution();
		try{
			init();									// init
			assignParents(workflow.get(workflow.size() - 1));	// parent assign for exit task
	
			// allocate entry and exit tasks
			solution.addTaskToVMStart(getEarliestVM(), wf.getEntryTask(), 0);  // 后一个0表示插入位置
			solution.addTaskToVMEnd(getLatestVM(), wf.getExitTask(), solution.getMakespan());
			
			return solution;
		}catch(RuntimeException e){
			//it means ICPCP fails to yield a solution meeting the deadline. This is because 'assignPath' may fail
			return null;
		}
	}
	
	private void init(){					//Algorithm 1 in the paper; for cases of initialization and update
		TaskWrapper entryTask = workflow.get(0);
		entryTask.setAST(0);
		entryTask.setAFT(0);
		entryTask.setAssigned(true);
		
		for(int i=1; i<workflow.size(); i++){		// compute EST, EFT, critical parent via Eqs. 1 and 2; skip entry task
			TaskWrapper task = workflow.get(i);

			//此处EST定义不考虑resource的available time，且还要计算critical parent；所以没有使用solution.calcEST方法
			double EST = -1;
			double ESTForCritical = -1;
			TaskWrapper criticalParent = null;		
			for(Edge e: task.getTask().getInEdges()){
				TaskWrapper parent = convertToWrapper(e.getSource()) ;
				double startTime = e.getDataSize()/VM.NETWORK_SPEED;
				//if assigned, use AFT; otherwise, use EFT
				startTime += parent.isAssigned() ? parent.getAFT() : parent.getEFT();
				EST = Math.max(EST, startTime);				//determine EST
				if(startTime > ESTForCritical && parent.isAssigned()==false){	//determine critical parent
					ESTForCritical = startTime;
					criticalParent = parent;
				}
			}
			if(task.isAssigned() == false){
				task.setEST(EST);
				task.setEFT(EST + task.getTask().getTaskSize() / bestVMSpeed);
			}
			//分配了的还需要更新critical parent:因为task a在assignParents，可能有两个parent b和c，所以必须要更新了
			task.setCriticalParent(criticalParent);	
		}

		TaskWrapper exitTask = workflow.get(workflow.size()-1);	//Note, EST, EFT, critialParent of exitTask have been set above
		exitTask.setAFT(deadline);
		exitTask.setAST(deadline);
		exitTask.setAssigned(true);
		for(int j = workflow.size() - 2; j>=0; j--){	// compute LFT via Eq. 3; reverse order, skip exit node
			TaskWrapper task = workflow.get(j);
			if(task.isAssigned())
				continue;
			
			double lft = Double.MAX_VALUE;
			for(Edge e : task.getTask().getOutEdges()){
				TaskWrapper child = convertToWrapper(e.getDestination());
				double finishTime;
				if(child.isAssigned())	
					finishTime = child.getAST() - e.getDataSize() / VM.NETWORK_SPEED;
				else
					finishTime = child.getLFT() - child.getTask().getTaskSize()/bestVMSpeed - e.getDataSize() / VM.NETWORK_SPEED;
				lft = Math.min(lft, finishTime);
			}
			task.setLFT(lft);
		}
	}
	
	private void assignParents(TaskWrapper task){			//Algorithm 2 in the paper
		while(task.getCriticalParent() != null){	
			List<TaskWrapper> PCP = new ArrayList<TaskWrapper>();
			TaskWrapper ti = task;
			while(ti.getCriticalParent() != null){		// while (there exists an unassigned parent of ti)
				PCP.add(0, ti.getCriticalParent());   	//add CriticalParent(ti) to the beginning of PCP
				ti = ti.getCriticalParent();
			}
			assignPath(PCP);	//path assign
			init();				//re-init, i.e., update in the paper
			for(TaskWrapper tj : PCP)	//call AssignParents(ti)
				assignParents(tj);
		}
	}
	
	//choose the cheapest service for PCP; 从existing和new VM中一起寻找最便宜的；论文里的只要existing里找到就停止
	private void assignPath(List<TaskWrapper> PCP){	//Algorithm 3 in the paper; the actual situation is more complex
		double minExtraCost = Double.MAX_VALUE;	//the criterion to select VM
		List<TAllocation> bestList = null;
		
aa:		for(VM vm : solution.getUsedVMSet()){			//search from existing VMs. 必要条件：1.使用时间不冲突；2.满足LFT
			List<TAllocation> tmpList = new ArrayList<TAllocation>();
			for(int i = 0; i<PCP.size(); i++){		
				TaskWrapper task = PCP.get(i);
				double taskEST = task.getEST();	
				if(i > 0)
					taskEST = Math.max(taskEST, tmpList.get(i-1).getFinishTime());
				if(taskEST + task.getTask().getTaskSize() / vm.getSpeed() > task.getLFT() + Config.EPS)//lft is not met, skip vm
					continue aa;
				
				double startTime = searchStartTime(vm, task.getTask(), taskEST, task.getLFT());	//how to put task onto vm
				if(startTime != -1)
					tmpList.add(new TAllocation(vm, task.getTask(), startTime));
				else
					continue aa;
			}
			//费用计算，这里是整个vm一直保持alive的计算方式
			double newTotalUsedTime = Math.max(tmpList.get(tmpList.size() - 1).getFinishTime(), solution.getVMLeaseEndTime(vm))
						- Math.min(tmpList.get(0).getStartTime(), solution.getVMLeaseStartTime(vm));
			double extraCost = Math.ceil(newTotalUsedTime / VM.INTERVAL) * vm.getUnitCost() - solution.getVMCost(vm); //oldVMTotalCost
			if(extraCost<minExtraCost){	
				minExtraCost = extraCost;
				bestList = tmpList;
			}
		}
		
		int selectedI = -1;
		for(int i = 0; i<VM.TYPE_NO; i++){		// try new VMs
			List<TAllocation> tmpList = new ArrayList<TAllocation>();
			boolean isSatisfied = true;
			for(int k = 0; k<PCP.size(); k++){
				TaskWrapper task = PCP.get(k);
				double taskEST = task.getEST();	
				if(k > 0)
					taskEST = Math.max(taskEST, tmpList.get(k-1).getFinishTime());
				if(taskEST + task.getTask().getTaskSize() / VM.SPEEDS[i] > task.getLFT() + Config.EPS){	//lft is not met
					isSatisfied = false;
					break;
				}
				tmpList.add(new TAllocation(i, task.getTask(), taskEST));
			}
			if(isSatisfied){
				double extraCost = Math.ceil((tmpList.get(tmpList.size() - 1).getFinishTime() - tmpList.get(0).getStartTime())/VM.INTERVAL)
						* VM.UNIT_COSTS[i];
				if(extraCost < minExtraCost){
					minExtraCost = extraCost;
					bestList = tmpList;
					selectedI = i;
				}
			}
		}
		if(selectedI != -1){
			VM vm = solution.newVM(selectedI);
			for(TAllocation e : bestList)
				e.setVM(vm);
		}
		if(bestList == null)		//fail to get a VM to support this PCP and thus fail to find a solution
			throw new RuntimeException();
		
		// schedule PCP on bestVM and set SS(task), AST(task)
		for(TAllocation alloc : bestList){	
			alloc.setFinishTime(alloc.getStartTime() + alloc.getTask().getTaskSize()/alloc.getVM().getSpeed());
			
			TaskWrapper task = convertToWrapper(alloc.getTask());
			task.setAssigned(true);		// set all tasks of P as assigned
			task.setAST(alloc.getStartTime());
			task.setAFT(alloc.getFinishTime());	
			solution.addTaskToVMEnd(alloc.getVM(), alloc.getTask(), alloc.getStartTime());
		}
		
		solution.sortTAListOnVM(bestList.get(0).getVM(), new Comparator<TAllocation>(){
			public int compare(TAllocation o1, TAllocation o2) {
				if(o1.getStartTime() > o2.getStartTime())
					return 1;
				else if(o1.getStartTime() < o2.getStartTime())
					return -1;
				return 0;
			}
		});	//sort allocations on VM; it is necessary
	}
	
	//search a time slot in vm between EST and LFT for task allocation
	//returning -1 means this task can not be placed to this vm between EST and LFT, in the target solution
	private double searchStartTime(VM vm, Task task, double EST, double LFT){
		List<TAllocation> list = solution.getTAListOnVM(vm);
		
		for(int i = 0;i<list.size()+1;i++){
			double timeSlotStart, timeSlotEnd;
			if(i == 0){
				timeSlotStart = 0;
				timeSlotEnd	= list.get(i).getStartTime();
			}else if(i==list.size()){
				timeSlotStart = list.get(i-1).getFinishTime();
				timeSlotEnd = Double.MAX_VALUE;
			}else{
				timeSlotStart = list.get(i-1).getFinishTime();
				timeSlotEnd	= list.get(i).getStartTime();
			}
			double slackTime = LFT - EST - task.getTaskSize()/vm.getSpeed();
			if(EST + slackTime >= timeSlotStart){			//condition1：startTime satisfies
				double startTime = Math.max(timeSlotStart, EST);
				//condition2：slot is large enough to support this task
				if(timeSlotEnd - startTime >= task.getTaskSize() / vm.getSpeed())	
					return startTime;
			}
		}
		return -1;
	}
	private VM getEarliestVM(){
		VM ealiestVM = null;
		double earliestTime = Double.MAX_VALUE;
		for(VM vm : solution.getUsedVMSet()){
			double startTime = solution.getVMLeaseStartTime(vm);
			if(startTime < earliestTime){
				earliestTime = startTime;
				ealiestVM = vm;
			}
		}
		return ealiestVM;
	}
	private VM getLatestVM(){
		VM latestVM = null;
		double latestTime = 0;
		for(VM vm : solution.getUsedVMSet()){
			double finishTime = solution.getVMLeaseEndTime(vm);
			if(finishTime > latestTime){
				latestTime = finishTime;
				latestVM = vm;
			}
		}
		return latestVM;
	}
	
	private TaskWrapper convertToWrapper(Task task){
		int id = wf.indexOf(task);
		return workflow.get(id);
	}
	
	/**
	 * TaskWrapper is used to add extra properties for the class Task, which is needed by ICPCP
	 * 包装比继承有时更好用，比如这里我如果用继承则需要重新构造；装饰模式(Decorator)(包装器模式(Wrapper))

	private class TaskWrapper{
		Task task;
		public Task getTask() {
			return task;
		}
		TaskWrapper(Task task){
			this.task = task;
		}
		
		//---------------------task properties used in ICPCP algorithm---------------------------
		private double EST = -1, EFT = -1, LFT = -1, AST = -1, AFT = -1;  //'-1' means the value has not been set
		private TaskWrapper criticalParent;
		private boolean isAssigned = false;		//assigned以后EST就表示实际的开始时间了；EFT和LFT都设为   finish time，与论文不同

		public double getEST() {		return EST;	}
		public void setEST(double eST) {		EST = eST;	}
		public double getEFT() {		return EFT;	}
		public void setEFT(double eFT) {		EFT = eFT;	}
		public double getLFT() {		return LFT;	}
		public void setLFT(double lFT) {		LFT = lFT;	}
		public TaskWrapper getCriticalParent() {	return criticalParent;	}
		public void setCriticalParent(TaskWrapper criticalParent) {	this.criticalParent = criticalParent;	}
		public boolean isAssigned() {		return isAssigned;	}
		public void setAssigned(boolean isAssigned) {		this.isAssigned = isAssigned;	}
		public double getAST() {		return AST;	}
		public void setAST(double aST) {		AST = aST;	}
		public double getAFT() {		return AFT;	}
		public void setAFT(double aFT) {		AFT = aFT;	}
	}
}
*/