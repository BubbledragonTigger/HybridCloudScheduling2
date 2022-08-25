package contentionFree.conOpt.methods;

import contentionFree.Solution;
import setting.Workflow;

public interface Scheduler {
	
	/**
	 * @param deadline-constrained cost optimization for @param wf workflow
	 */
	Solution schedule(Workflow wf, double deadline);
	
	
	// compare this solution to Solution s; if ==, returns false;
	// used for deadline-constrained optimization; deadline can be epsilonDeadline
	public static boolean isBetterThan(Solution s1, Solution s2, double deadline){
		double makespan1 = s1.getMakespan();
		double makespan2 = s2.getMakespan();
		double cost1 = s1.getCost();
		double cost2 = s2.getCost();
		
		if(makespan1 <= deadline && makespan2<= deadline ){	//both satisfy deadline
			return cost1<cost2;
		}else if(makespan1 > deadline && makespan2 > deadline ){//both does not satisfy
			return makespan1<makespan2;
		}else if(makespan1 <= deadline && makespan2 > deadline){ //this satisfy£¬s doesn't
			return true;
		}else if(makespan1 > deadline && makespan2 <= deadline) //this don't£¬s satisfies
			return false;
		
		return true;
	}
}
