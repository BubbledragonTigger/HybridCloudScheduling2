package contentionAware;
import setting.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
public class Evaluation {
    public static void main(String[] args) throws IOException {
        test("D:\\workflowSamples\\CYBERSHAKE\\CYBERSHAKE.n.50.2    .dax",false);  //真实工作流
        //test("D:\\example_7.dot",true);  //模拟工作流


    }

    private static String[] test(String file,boolean visualizeFlag)
    throws IOException{
        Workflow wf = new Workflow(file);
        List<CSolution> list = new ArrayList<CSolution>();

        CCSH ccsh = new CCSH();
        long t1 = System.currentTimeMillis();

        list.add(ccsh.listSchedule(wf, TProperties.Type.C_LEVEL, 1));
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
        return rtnValues;
    }

}
