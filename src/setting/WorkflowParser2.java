package setting;

import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class WorkflowParser2 {
	//------------------???JSON??? start----------------
	class CFile{
		String link, name, size;
	}
	class Job{
		String name, runtime;	//type
		ArrayList<String> children;//parents??????????????
		ArrayList<CFile> files; 
	}
	class WF{	//???????Workflow????????????
		ArrayList<Job> jobs;
//		String executedAt,makespan;	ArrayList<String> machines;
	}	
	class JSONFile{	//???????????????
		String name;	//?????????????
		WF workflow;	//?????workflow???????wf
//		Author author;	//class Author{ String name, email;}
//		String description, createdAt, schemaVersion;ArrayList<String>  wms;
	}
	public static List<Task> parseJSONFile(String file) throws IOException {
		Gson gson = new Gson();
		Reader reader = new FileReader(file);
		JSONFile json = gson.fromJson(reader, JSONFile.class);
		System.out.println("????????????"+json.name);
		 
		ArrayList<Job> jobs = json.workflow.jobs;
		 
		List<Task> list = new ArrayList<>();
		HashMap<String, Task> nameTaskMapping = new HashMap<String, Task>();
		HashMap<String, Job> nameJobMapping = new HashMap<String, Job>();
		for(Job job : jobs){	//????飬??????????tasks
			Task task = new Task(job.name, Double.parseDouble(job.runtime));
			list.add(task);
			nameTaskMapping.put(job.name, task);
			nameJobMapping.put(job.name, job);
		}
		for(Job job : jobs){	//????飬????task?????
			Task task = nameTaskMapping.get(job.name);
			List<CFile> outFiles = job.files.stream()
					.filter(e -> e.link.equals("output"))
					.collect(Collectors.toList());
//			outFiles.forEach(e -> System.out.println(e.link)); //test
			for(String child : job.children){
				Task childTask = nameTaskMapping.get(child);
				Job childJob = nameJobMapping.get(child);
				Edge e = new Edge(task, childTask);
				
				List<CFile> inFiles = childJob.files.stream()
						.filter(t -> t.link.equals("input"))
						.collect(Collectors.toList());
				double weight = 0;
				int i = 0;
				for(CFile outFile : outFiles){
					for(CFile inFile : inFiles){
						if(outFile.name.equals(inFile.name)){
							weight += Double.parseDouble(outFile.size);
							i++;
							break;
						}
					}
				}
//				System.out.println(i);	//	test??????????1
				
				e.setDataSize((long)(weight*1000));
				task.insertEdge(Task.TEdges.OUT, e);
				childTask.insertEdge(Task.TEdges.IN, e);
			}
		}
		WorkflowParser.addDummyTasks(list);
		return list;
	}
	//------------------???JSON??? end----------------
	
	public static void main(String[] args) throws IOException{
		parseJSONFile("D:\\seismology-workflow.json");
	}
}
