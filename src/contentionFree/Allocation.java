package contentionFree;

import java.util.List;

import static java.lang.Math.max;

/**
 * It has two concrete sub-classes: TAllocation, EAllocation. 
 * They are used for allocating tasks and edges, respectively
 * 这个类和 其   两个子类被迫设计成：其中的信息有可能被修改
 * @author wu
 */
public abstract class Allocation implements Comparable<Allocation>{
	protected double startTime;   //当前处理器的开始时间
	protected double finishTime;  //当前处理器的完成时间
	
	public double getStartTime() {
		return startTime;
	}   //获得当前处理器开始传输边时间
	public double getFinishTime() {
		return finishTime;
	}  //获得当前处理器完成传输边时间
	//used by ProLiS and ICPCP classes
	public void setStartTime(double startTime) {
		this.startTime = startTime;
	}   //设置当前处理器开始传输边时间
	//used by Solution when VM is upgraded, by Adaptor
	public void setFinishTime(double finishTime) {
		this.finishTime = finishTime;
	}  //设置当前处理器结束传输边时间
	
	public int compareTo(Allocation o) {            // 如果当前输入边的数据准备好的时间大于参数输入边准备好的时间，则返回1
		if(this.getStartTime() > o.getStartTime())
			return 1;
		else if(this.getStartTime() < o.getStartTime())
			return -1;
		return 0;
	}
	
	/**
	 * Search the earliest time slot in @param allocList from @param readyTime,
	 * which is no less than @param period
	 * 在allocList上从readytime开始，寻找最早的支持period长度的free time slot
	 */

	public static double searchFreeTimeSlot(List<? extends Allocation> allocList,  //寻找这个处理器空余间隙，注意是一个处理器
			double readyTime, double period ) {
		/*
		（i,j)i有好多
			readytime:是这个节点i完成时间，并不一定是开始边传输的时间，比如当前处理器一边传另一条边的数据，一边处理i节点。而且
			i节点处理完之后可能另一条边还没有传输完毕
			period：边传输时间
		 */
		if(allocList == null || allocList.size() ==0)
			return readyTime;

		if(readyTime + period <= allocList.get(0).getStartTime()){// case1: 插入在最前面，#此时所有的输入边还没有开始传输通信
			return readyTime;
		}
		double EST = 0;
		for(int j = allocList.size() - 1; j >= 0 ; j--){
			Allocation alloc = allocList.get(j);//放在alloc的后面
			double startTime = max(readyTime, alloc.getFinishTime());   //
			double finishTime = startTime + period;
			
			if(j == allocList.size() - 1){			// case2: 放在最后面，EST肯定可以传输边，在慢慢寻找间隙
				EST = startTime;             //EST也是处理器最早可以传输边的时间
			}else {								//case3: 插入到中间。alloc不是最后一个，还存在allocNext；需test能否插入
				Allocation allocNext = allocList.get(j+1);
				if(finishTime > allocNext.getStartTime())//就是结束的时间大于了后面另一条边开始传输的时间，存在overlap无法插入
					continue;

				EST = startTime;//否则就可以,就是结束的时间小于了后面另一条边开始传输的时间
			}
			if(readyTime>alloc.getFinishTime())	//终结循环，因为在readTime之后的都已经尝试完了，前面的alloc的空余间隙都小于readytime，
												//肯定不行
				break;
		}
		return EST;
	}
}
