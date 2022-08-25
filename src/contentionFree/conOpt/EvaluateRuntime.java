package contentionFree.conOpt;

import contentionFree.LSUtil;
//import contentionFree.conOpt.methods.LACO;
//import contentionFree.conOpt.methods.PSO;
import contentionFree.conOpt.methods.Scheduler;
import setting.Workflow;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
/*
public class EvaluateRuntime {
	private static final double DEADLINE_FACTOR = 0.2; 
	private static final int FILE_INDEX_MAX = 1;  //10
	private static final String[] WORKFLOWS = { "GENOME", "CYBERSHAKE", "LIGO", "MONTAGE"};//

	private static final Scheduler[] METHODS = { new PSO(),  new LACO()}; //new PSO(),  new LACO(),
	private static final int FILE_SIZE_MAX = 4;  //10
	
	public static void main(String[] args) throws Exception {
		
		long[][] runtime = new long[FILE_SIZE_MAX][METHODS.length];
		for(int fileSizeIndex = 0; fileSizeIndex<FILE_SIZE_MAX; fileSizeIndex++){
			int size = 100 * (fileSizeIndex+1);
			for(int methodIndex = 0; methodIndex < METHODS.length; methodIndex++){
				for(int typeIndex = 0;typeIndex<WORKFLOWS.length;typeIndex++){
					String workflow = WORKFLOWS[typeIndex];
					for(int fileNumIndex = 0;fileNumIndex<FILE_INDEX_MAX;fileNumIndex++){
						String file = Config.getWorkflowLocation() + "\\" + workflow + 
								"\\" + workflow + ".n." + size + "." + fileNumIndex + ".dax";

						Workflow wf = new Workflow(file);	
						LSUtil benSched = new LSUtil(wf);
						double deadline = benSched.getFastSchedule().getMakespan() + (benSched.getCheapSchedule().getMakespan()
								- benSched.getFastSchedule().getMakespan())* DEADLINE_FACTOR;
						
						long t1 = System.currentTimeMillis();
						METHODS[methodIndex].schedule(wf, deadline);
						runtime[fileSizeIndex][methodIndex] += System.currentTimeMillis() - t1;
					}
				}
				runtime[fileSizeIndex][methodIndex] /= WORKFLOWS.length * FILE_INDEX_MAX;
			}
		}

		
		BufferedWriter bw = new BufferedWriter(new FileWriter(new File(Config.getOutputLocation() + "\\runtime.txt")));
		for(int fileSizeIndex = 0; fileSizeIndex<FILE_SIZE_MAX; fileSizeIndex++){
			int size = 100 * (fileSizeIndex+1);
			bw.write(size +"\t");
			for(int methodIndex = 0; methodIndex < METHODS.length; methodIndex++)
				bw.write(runtime[fileSizeIndex][methodIndex]+"\t"); 
			bw.write("\r\n");
		}
		bw.flush();
		bw.close();
	}
}


 */