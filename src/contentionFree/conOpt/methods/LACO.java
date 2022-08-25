package contentionFree.conOpt.methods;

import contentionFree.LSUtil;
import contentionFree.Solution;
import setting.Edge;
import setting.TProperties;
import setting.Task;
import setting.Workflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.lang.Math.pow;
import static java.lang.Math.random;

/**
 * the L-ACO algorithm
 */
/*
public class LACO implements Scheduler {
	private static final double ALPHA = 1;
	private static final double BETA = 2;
	private static final double EVAP_RATIO = 0.8;
	private static final int NO_OF_ITE = 50;
	private static final int NO_OF_EPSILON_ITE = (int)(NO_OF_ITE*0.7);
	private static final int NO_OF_ANTS = 20;
	
	private double[][] pheromone; 
	private double[] heuristic;
	private Workflow wf;
	private ProLiS pds = new ProLiS(1.5);
	
	private double epsilonDeadline;
	
	@Override
	public Solution schedule(Workflow wf, double deadline) {
		this.wf = wf;
		int size = wf.size();
		heuristic = new double[size];
		pheromone = new double[size][size];
		for(int i =0;i<size;i++)		//initialize pheromone
			for(int j=0;j<size;j++)
				pheromone[i][j] = 1;

		HashMap<Task, Double> pURanks = new TProperties(wf, TProperties.Type.PU_RANK, pds.getTheta());
		LSUtil bench = new LSUtil(wf);
		double maxMakespan = bench.getCheapSchedule().getMakespan();//used to calculate epsilonDeadline
		Ant gbAnt = null;	//globalBestAnt
		for(int iterIndex = 0; iterIndex<NO_OF_ITE; iterIndex++){	 //iteration index
			Ant[] ants = new Ant[NO_OF_ANTS];
			for(Task t : wf.getTaskList())	//initialize heuristic information
				heuristic[wf.indexOf(t)] = pURanks.get(t);
			
			if(maxMakespan<deadline || iterIndex >= NO_OF_EPSILON_ITE)
				epsilonDeadline = deadline;
			else
				epsilonDeadline = deadline +
					(maxMakespan-deadline)* Math.pow((1-(double)iterIndex/NO_OF_EPSILON_ITE), 4);
			Ant lbAnt = null;	//localBestAnt
			for(int antId = 0;antId<NO_OF_ANTS;antId++){
				ants[antId] = new Ant();
				ants[antId].constructASolution();
				if(lbAnt==null || Scheduler.isBetterThan(ants[antId].solution, lbAnt.solution, epsilonDeadline))
					lbAnt = ants[antId];
			}
			
			//update  pheromone
			for(int j =0;j<size;j++)	
				for(int i=0;i<size;i++)
					pheromone[j][i] *= EVAP_RATIO;
			if(gbAnt!=null && random()>0.9)
				gbAnt.releasePheromone();
			else
				lbAnt.releasePheromone();
			for(int j =0;j<size;j++){
				for(int i=0;i<size;i++){
					if(pheromone[j][i]>1)
						pheromone[j][i]=1;
					else if(pheromone[j][i]<0.2)
						pheromone[j][i]=0.2;
				}
			}
			
			if(gbAnt==null || Scheduler.isBetterThan(lbAnt.solution, gbAnt.solution, epsilonDeadline)){
				gbAnt = lbAnt;
				System.out.printf("Iteration index£º%3d\t%5.2f\t%5.2f\t%5.2f\n",iterIndex,
						gbAnt.getSolution().getCost(),
						gbAnt.getSolution().getMakespan(),epsilonDeadline);
			}
		}
		return gbAnt.getSolution();
	}
	
    private class Ant {
		private Solution solution;
		private int[] taskIdList = new int[wf.size()];
		
		public Solution constructASolution(){
    		List<Task> L = new ArrayList<Task>();	//Empty list that will contain the sorted elements
    		List<Task> S = new ArrayList<Task>();	//S: Set of all nodes with no incoming edges	
    		S.add(wf.getEntryTask());		

    		HashMap<Task, Integer> inEdgeCounts = new HashMap<>();
    		int tIndex = 0;			//task index in task ordering L
    		while(S.size()>0){
    			Task task;       
    			// remove a task from S
    			if(tIndex==0)	
    				task = S.remove(0);	//entry task
    			else
    				task = chooseNextTask(taskIdList[tIndex], S);
    			
    			taskIdList[tIndex] = wf.indexOf(task);
    			tIndex++;
    			L.add(task);					// add n to tail of L
        		
    			for(Edge e : task.getOutEdges()){	// for each node m with an edge e from n to m do
    				Task child = e.getDestination();
    				Integer count = inEdgeCounts.get(child);
    				inEdgeCounts.put(child, count != null ? count+1 : 1);
    				if(inEdgeCounts.get(child) == child.getInEdges().size())	//  if m has no other incoming edges then
    					S.add(child);					// insert m into S			
    			}
    		}

    		solution =  pds.buildViaTaskList(wf, L, epsilonDeadline);
    		return solution;
    	}
        
        private Task chooseNextTask(int curTaskId, List<Task> S) {
            double sum = 0;		
            for (Task t : S)	
                sum += pow(pheromone[curTaskId][wf.indexOf(t)], ALPHA) * pow(heuristic[wf.indexOf(t)], BETA);
            
            double slice = sum * random();
            double k = 0;			
            int chosenIndex = 0;			//the chosen index in S
            for (int indexInS = 0; k < slice; indexInS++) {	
            	Task t = S.get(indexInS);
                k += pow(pheromone[curTaskId][wf.indexOf(t)], ALPHA) * pow(heuristic[wf.indexOf(t)], BETA);
                chosenIndex = indexInS;
            }
            return S.remove(chosenIndex);
        }
    	
        public void releasePheromone() {
        	double value = 1 / solution.getCost() + 0.5;
        	for(int i = 0;i<taskIdList.length-1; i++)
        		pheromone[taskIdList[i]][taskIdList[i+1]] += value;
        }

    	public Solution getSolution() {
			return solution;
		}

		@Override
		public String toString() {
			return "Ant [cost=" + solution.getCost() + ", makespan=" + solution.getMakespan()+ "]";
		}
    }
}

 */