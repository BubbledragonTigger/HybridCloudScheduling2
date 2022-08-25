package contentionFree.conOpt.methods;

import contentionFree.LSUtil;
import contentionFree.Solution;
import contentionFree.TAllocation;
import setting.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

/**
 * the ProLiS algorithm
 */
/*
public class ProLiS implements Scheduler {
	
	private double theta = 2;
	public ProLiS(double theta){
		this.theta = theta;
	}
	public double getTheta() {
		return theta;
	}
	
	public Solution schedule(Workflow wf, double deadline) {
		List<Task> tasks = new ArrayList<Task>(wf.getTaskList());
		Collections.sort(tasks, new TProperties(wf, TProperties.Type.PU_RANK, theta)); 	
		Collections.reverse(tasks);	//sort based on pURank, larger first
		
		return buildViaTaskList(wf, tasks, deadline);
	}
	
	//build a solution based on a task ordering.
	//that is, for a given task ordering, distribute deadline and select services here
	Solution buildViaTaskList(Workflow wf, List<Task> tasks, double deadline) {
//		int violationCount = 0;		// test code
		Solution solution = new Solution();
		double CPLength = wf.getCPLength(); 	//critical path
		HashMap<Task, Double> pURanks = new TProperties(wf, TProperties.Type.PU_RANK, theta);
		
		for(int i = 1; i < tasks.size(); i++){		
			Task task = tasks.get(i);
			double proSubDeadline = (CPLength - pURanks.get(task) + task.getTaskSize()/VM.SPEEDS[VM.FASTEST])
							/CPLength * deadline;
			TAllocation alloc = getMinCostVM(task, solution,proSubDeadline, i);

			//当CPLength>deadline时，子期限的划分可能导致EFT>subDeadline；所以必须考虑子期限不满足的情况：此时选择minimal EFT的VM
			if(alloc == null){			//select a vm which allows EFT
				alloc = getMinEFTVM(task, solution, proSubDeadline, i);
				
				VM vm = alloc.getVM();
				while(alloc.getFinishTime() > proSubDeadline + Config.EPS && vm.getType() < VM.FASTEST){
					upgradeVM(solution, vm);			//upgrade若进行整个解的更新；复杂度将增长太多。
					alloc.setStartTime(LSUtil.calcEST(solution,task, vm));
					alloc.setFinishTime(LSUtil.calcEST(solution,task, vm) + task.getTaskSize()/vm.getSpeed());
				}
//				if(alloc.getFinishTime() > proSubDeadline + Config.E)
//					violationCount ++;
			}
			if(i == 1)		//after allocating task_1, allocate entryTask to the same VM 
				solution.addTaskToVMEnd(alloc.getVM(), tasks.get(0), alloc.getStartTime());
			solution.addTaskToVMEnd(alloc.getVM(), task, alloc.getStartTime());	//allocate
		}
//		if(violationCount > 0)
//			System.out.println("Number of sub-deadline violation: " + violationCount);
		
		return solution;
	}
	
	//used only by ProLiS class
	private void upgradeVM(Solution solution, VM vm){
		//这里仅进行该VM上的更新，其他的vm的就不再涉及了——应该和Solution的clone方法一样重新插入
		vm.setType(vm.getType()+1);
		List<TAllocation> list = solution.getTAListOnVM(vm);
		if(list == null)
			return;
		for(TAllocation alloc : list){
			double newFinishTime = alloc.getTask().getTaskSize() / vm.getSpeed() + alloc.getStartTime();
			alloc.setFinishTime(newFinishTime);
		}
	}
	
	// select a vm that meets sub-deadline and minimizes the cost
	//candidate services include all the services that have been used (i.e., R), 
	//			and those that have not been used but can be added any time (one service for each type)
	private TAllocation getMinCostVM(Task task, Solution solution, double subDeadline, int taskIndex){
		double minIncreasedCost = Double.MAX_VALUE;	//increased cost for one VM is used here, instead of total cost
		VM selectedVM = null;
		double selectedStartTime = 0;
		
		double maxOutTime = 0;	//maxTransferOutTime
		for(Edge e : task.getOutEdges())
			maxOutTime = Math.max(maxOutTime, e.getDataSize());
		maxOutTime /= VM.NETWORK_SPEED;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that meets sub-deadline and minimizes the cost
		for(VM vm : solution.getUsedVMSet()){	
			startTime = LSUtil.calcEST(solution,task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime > subDeadline + Config.EPS)   //sub-deadline not met
				continue;
			
			double newVMPeriod = finishTime + maxOutTime - solution.getVMLeaseStartTime(vm);
			double newVMTotalCost = Math.ceil(newVMPeriod/VM.INTERVAL) * vm.getUnitCost();
			double increasedCost = newVMTotalCost - solution.getVMCost(vm);  // oldVMTotalCost
			if(increasedCost < minIncreasedCost){ 
				minIncreasedCost = increasedCost;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}

		//test whether a new VM can meet the sub-deadline and (or) reduce increasedCost; if so, add this new VM
		int selectedI = -1;				
		startTime = taskIndex==1 ? VM.LAUNCH_TIME : LSUtil.calcEST(solution,task, null);
		for(int k = 0 ; k<VM.TYPE_NO; k++){
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[k];
			if(finishTime > subDeadline + Config.EPS)	//sub-deadline not met
				continue;
			
			double increasedCost = Math.ceil((finishTime - startTime)/VM.INTERVAL) * VM.UNIT_COSTS[k];
			if(increasedCost < minIncreasedCost){
				minIncreasedCost = increasedCost;
				selectedI = k;
				selectedStartTime = startTime;
			}
		}
		if(selectedI != -1)
			selectedVM = solution.newVM(selectedI);
		
		if(selectedVM == null)
			return null;
		else
			return new TAllocation(selectedVM, task, selectedStartTime);
	}
	
	//select a VM from R which minimizes the finish time of the task
	//here, candidates only include services from R if R is not null
	private TAllocation getMinEFTVM(Task task, Solution solution, double subDeadline, int taskIndex){
		VM selectedVM = null;				
		double selectedStartTime = 0;
		double minEFT = Double.MAX_VALUE;
		
		double startTime, finishTime;
		// traverse VMs in solution to find a vm that minimizes EFT
		for(VM vm : solution.getUsedVMSet()){			
			startTime = LSUtil.calcEST(solution,task, vm); 
			finishTime = startTime + task.getTaskSize()/vm.getSpeed();
			if(finishTime < minEFT){
				minEFT = finishTime;
				selectedVM = vm;
				selectedStartTime = startTime;
			}
		}

		// if solution has no VMs 
		if(selectedVM==null ){		// logically, it is equal to "solution.keySet().size()==0"
			startTime = taskIndex==1 ? VM.LAUNCH_TIME : LSUtil.calcEST(solution,task, null);
			finishTime = startTime + task.getTaskSize()/VM.SPEEDS[VM.FASTEST];
			if(finishTime < minEFT){
				minEFT = finishTime;
				selectedStartTime = startTime;
				selectedVM = solution.newVM(VM.FASTEST);
			}
		}
		return  new TAllocation(selectedVM, task, selectedStartTime);
	}
}

 */