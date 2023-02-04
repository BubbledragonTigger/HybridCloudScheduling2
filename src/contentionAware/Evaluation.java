package contentionAware;
import setting.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class Evaluation {
    public static void main(String[] args) throws IOException {
        // test("D:\\workflowSamples\\MONTAGE\\MONTAGE.n.100.0.dax",false);  //真实工作流
        //多个算法使用求averageMakespan
        List<Double> makespan = new ArrayList<>();
        for(int i = 0;i<20;i++){
            ProjectCofig.seed = i;
            makespan.add(test(ProjectCofig.path,false));
        }
        System.out.println("mean:"+mean(makespan));
        System.out.println("std:"+standardDeviaction(makespan));
        //单个算法使用
        //test(ProjectCofig.path,false);  //真实工作流



    }

    private static Double test(String file,boolean visualizeFlag)
    throws IOException{
        Workflow wf = new Workflow(file);
        List<CSolution> list = new ArrayList<CSolution>();

        CCSH ccsh = new CCSH();
        long t1 = System.currentTimeMillis();


        list.add(ccsh.listSchedule(wf, ProjectCofig.type,ProjectCofig.adaptorType));
        long t2 = System.currentTimeMillis();

        System.out.println("runTime: " +  (t2-t1));
        String result ="";

        String runtime = "";
//        for(CSolution c : list){
//            result += c.getMakespan()+"\t"+c.getCost() + "\t";
//            System.out.println(c.getMakespan()+"\t"+c.getCost() + "\t"+ c.validate(wf));
//            if(visualizeFlag)
//                ChartUtil.visualizeScheduleNew(c);
//        }
        //used for data collection
        result += wf.getSequentialLength() +"\t" + wf.getCPTaskLength()+"\t" + wf.getCCR();
        System.out.println(result);

        String[] rtnValues = {result, runtime};
        list.clear();

        return ccsh.getMakespan();
    }



    public static double standardDeviaction(List<Double> list){
        double sum = 0;
        double meanValue = mean(list);                //平均数
        for(int i = 0;i < list.size();i++){
            sum += Math.pow(list.get(i)-meanValue, 2);
        }
        return Math.sqrt(sum/list.size());
    }

    //计算和
    public static double calcSum(List<Double> list){
        double sum = 0;
        for(int i = 0;i<list.size();i++){
            sum += list.get(i);
        }
        return sum;
    }

    //求平均值
    public static double mean(List<Double> list){
        return calcSum(list) / list.size();
    }


}
