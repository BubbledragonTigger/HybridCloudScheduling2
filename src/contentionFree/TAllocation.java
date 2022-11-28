package contentionFree;

import setting.Task;
import setting.VM;
import setting.VM_Private;
import setting.VM_Public;

import java.util.Objects;

/**
 * Task Allocation Information
 * @author wu
 */
public class TAllocation extends Allocation {

	private Task task;
	private VM vm;

	public VM_Private getPrivateVM() {
		return privateVM;
	}

	public void setPrivateVM(VM_Private privateVM) {
		this.privateVM = privateVM;
	}

	public VM_Public getPublicVM() {
		return publicVM;
	}

	public void setPublicVM(VM_Public publicVM) {
		this.publicVM = publicVM;
	}

	private VM_Private privateVM;
	private VM_Public publicVM;
	protected TAllocation(){}
	public TAllocation(VM_Public publicVM,VM_Private privateVM, Task task, double startTime,double speed) {
		this.task = task;
		this.startTime = startTime;
		this.publicVM = publicVM;
		this.privateVM = privateVM;

		this.finishTime = startTime + task.getTaskSize() / speed;
	}

	//-------------------------------------getters & setters--------------------------------
	public VM getVM(){
		return vm;
	}

	public Task getTask() {
		return task;
	}
	
	//-------------------------------------overrides--------------------------------
	// 这里不加入VM信息，只是为了Solution在toString时更清晰
	public String toString() {
		return "Allocation [task=" + task.getName() + ": "+ startTime
				+ ", " + finishTime + "]";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null ) return false;
		TAllocation that = (TAllocation) o;
		return task.getName().equals(task.getName()) &&
				vm.getAttribute().equals(that.getVM().getAttribute())
				&& vm.getId().equals(that.getVM().getId())
				&&Objects.equals(startTime, that.startTime)
				&&Objects.equals(finishTime, that.finishTime);
	}

	@Override
	public int hashCode() {
		return Objects.hash(task, vm);
	}
	/*
	@Override
    public boolean equals(Object obj) {
		TAllocation a2=(TAllocation)obj;
		if(a2 == null)
			return false;
		
		boolean flag = task == a2.task;
		flag &= (vm.getType() == a2.vm.getType())
				&&vm.getId() == a2.vm.getId();
		flag &= startTime == a2.startTime;
		if(flag)
			return true;
		return false;
    }
	//定义了equals方法后，hashcode方法的定义成了必须
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((task == null) ? 0 : task.hashCode());
		result = prime * result + ((vm == null) ? 0 : vm.getType());
		result = prime * result + ((vm == null) ? 0 : vm.getId());
		result = result + (int)(prime * startTime);
		return result;
	}

	 */

	//-------------------------------------only for ICPCP---------------------------
    @Deprecated
    public TAllocation(VM vm, Task task, double startTime) {
		this.vm = vm;
		this.task = task;
		this.startTime = startTime;
		//this.finishTime = startTime + task.getTaskSize() / VM.SPEEDS[vmId];
		if(task.getRunOnPrivateOrPublic() == true){
			this.finishTime = startTime + task.getTaskSize() / VM.SPEEDS[VM.SLOWEST];
		}
		else{
			this.finishTime = startTime + task.getTaskSize() / VM.SPEEDS[VM.FASTEST];
		}

	}
    @Deprecated
	public void setVM(VM vm) {
		this.vm = vm;
	}
	//-------------------------------------only for ICPCP---------------------------
}