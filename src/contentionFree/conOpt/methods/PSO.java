package contentionFree.conOpt.methods;

import contentionFree.LSUtil;
import contentionFree.Solution;
import setting.Edge;
import setting.Task;
import setting.VM;
import setting.Workflow;

import java.util.*;

/**
 * Rodriguez, Maria Alejandra, and Rajkumar Buyya. "Deadline based resource provisioning and scheduling
 * algorithm for scientific workflows on clouds." IEEE Transactions on Cloud Computing 2.2 (2014): 222-235.
 */
/*
public class PSO implements Scheduler {

	private static final int POPSIZE = 100;
	private static final int NO_OF_ITE = 100;
	private static final double W = 0.5f, C1 = 2f, C2 = 2f;		//parameters for PSO are from the paper
	
	private Workflow wf;
	private double deadline; 
	private int range;
	private Random rnd = new Random();
	
	private int dimension;	//number of tasks
	private VM[] vmPool;
	
	@Override
	public Solution schedule(Workflow wf, double deadline) {
		int maxParallel = getMaxParallel(wf.getEntryTask());
		this.wf = wf;
		this.deadline = deadline;
		this.dimension = wf.size();
		this.range = maxParallel * VM.TYPE_NO;
		this.vmPool = new VM[range];
		Solution solution = new Solution();  //这行代码只在接下去new VM中用到
		for(int i = 0; i < vmPool.length; i++){
			vmPool[i] = solution.newVM(i/maxParallel); // in vmPool, VMType ascends
		}
		
		double xMin = 0,  xMax = range - 1;	//boundary
		double vMax = xMax;					//maximum velocity
		double[] globalBestPos = new double[dimension];	//global Best Position
		Solution globalBestSol = null;		
		
		Particle[] particles = new Particle[POPSIZE];
		for (int i = 0; i < POPSIZE; i++){		//initialize particles 
			particles[i] = new Particle(vMax, xMin, xMax);
			particles[i].generateSolution();
			
			if (globalBestSol == null || Scheduler.isBetterThan(particles[i].sol, globalBestSol, deadline)) {
				for (int j = 0; j < dimension; j++)
					globalBestPos[j] = particles[i].position[j];
				globalBestSol= particles[i].sol;	// 这里不需要clone，因为particle的sol每次迭代时都会重新new的
			}
		}
		System.out.println("the best initial solution:"+globalBestSol.getCost()+";\t"+globalBestSol.getMakespan());
		
		for (int iteIndex = 0; iteIndex < NO_OF_ITE; iteIndex++) {
//			W = (double) (1.0 - iteIndex * 0.6 / 499);	//惯性递减，比w = 1效果要好一些。
			for (int i = 0; i < POPSIZE; i++) {
				for (int j = 0; j < dimension; j++) {
					particles[i].speed[j] = W * particles[i].speed[j]
					        + C1 * rnd.nextDouble() * (particles[i].bestPos[j] - particles[i].position[j])
							+ C2 * rnd.nextDouble() * (globalBestPos[j] - particles[i].position[j]);  //全局最好位置作为邻居
					particles[i].speed[j] = Math.min(particles[i].speed[j], vMax);
					
					particles[i].position[j] = particles[i].position[j] + particles[i].speed[j];

					particles[i].position[j] = Math.max(particles[i].position[j], xMin);	//bound
					particles[i].position[j] = Math.min(particles[i].position[j], xMax);
				}
				particles[i].generateSolution();
				//record a better solution
				if (globalBestSol == null || Scheduler.isBetterThan(particles[i].sol, globalBestSol, deadline)) {
					for (int j = 0; j < dimension; j++)
						globalBestPos[j] = particles[i].position[j];
					globalBestSol= particles[i].sol;
					
					System.out.printf("Iteration index：%3d\t%5.2f\t%5.2f\n",iteIndex,
							globalBestSol.getCost(),	globalBestSol.getMakespan());
				}
			}
		}
		System.out.println("Globle best is :" + globalBestSol.getCost()+";\t"+globalBestSol.getMakespan());
		return globalBestSol;
	}
	
	// based on Kahn algorithm, calculate maximal parallel number
	public int getMaxParallel(Task entry) {// an approximate maximal parallel branches
		// Empty list that will contain the sorted elements
		final List<Task> topoList = new ArrayList<Task>();	
		//S←Set of all nodes with no incoming edges
		//这里用优先队列是为了求近似的maxParallel. because of using PriorityQueue, here the comparison is reverse
		PriorityQueue<Task> S = new PriorityQueue<Task>(new Comparator<Task>(){
			public int compare(Task t1, Task t2) {
				int tmp1 = t1.getOutEdges().size() - t1.getInEdges().size();
				int tmp2 = t2.getOutEdges().size() - t2.getInEdges().size();
				return -1 * Double.compare(tmp1, tmp2);
			}
		});
		S.add(entry);		

		HashMap<Task, Integer> inEdgeCounts = new HashMap<>();
		int maxParallel = -1;
		while(S.size()>0){
			maxParallel = Math.max(maxParallel, S.size());
			Task task = S.poll();		// remove a node n from S
			topoList.add(task);			// add n to tail of L
			for(Edge e : task.getOutEdges()){	// for each node m with an edge e from n to m do
				Task t = e.getDestination();
				Integer count = inEdgeCounts.get(t);
				inEdgeCounts.put(t, count != null ? count+1 : 1);
				if(inEdgeCounts.get(t) == t.getInEdges().size())	//if m has no other incoming edges then
					S.add(t);					// insert m into S			
			}
		}
		// It is a low bound and a larger one may exists
		System.out.println("An approximate value for maximum parallel number: " + maxParallel); 
		return maxParallel;	
	}
	
	private class Particle{
		private double[] position = new double[dimension];
		private double[] speed = new double[dimension];
		private double[] bestPos = new double[dimension];
		private Solution sol, bestSol = null;
		
		//initialize a particle
		public Particle(double vMax, double xMin, double xMax){
			for (int i = 0; i < dimension; i++){
				this.position[i] = rnd.nextDouble() * (xMax - xMin) + xMin; 
				this.speed[i] = vMax * rnd.nextDouble() - vMax/2;			
				this.bestPos[i] = this.position[i];	
			}
		}
		
		public void generateSolution() {		//generate solution from position
			this.sol = new Solution();	
			for(int i=0;i<position.length;i++){
				Task task = wf.get(i);		// tasks in wf is a topological sort
				int vmIndex = (int)(Math.floor(position[i]));
				VM vm = vmPool[vmIndex];
				double startTime = LSUtil.calcEST(sol,task, vm);
				sol.addTaskToVMEnd(vm, task, startTime);
			}
			
			//record the best solution this particle has found
			if (bestSol==null || Scheduler.isBetterThan(this.sol, bestSol, deadline)){
				for (int j = 0; j < dimension; j++)
					this.bestPos[j] = this.position[j];	
				this.bestSol = this.sol;	
			}
		}

		public String toString() {
			if(sol != null)
				return "Particle [" + sol.getCost()+ ", " + sol.getMakespan()+ "]";
			return "";
		}
	}
}

 */