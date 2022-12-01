package setting;
import java.util.*;
public class Task{


    public static enum TEdges{IN,OUT};
    private String name;
    private double taskSize;

    private double AST;   //Actual Start Time
    private double AFT;   //Actual Finish Time
    private double EST;   //Earliest execution Start Time
    private double EFT;   //Earliest execution Finish Time

    private VM_Private priavteVM;
    private VM_Public publicVM;

    private boolean RunOnPrivateOrPublic;
    //adjacent list to store edge information
    //用于存储边缘信息的相邻列表
    //由于子边的终端之间可能也存在父子关系，所以这些edge都是按照其终端对应的拓扑顺序进行排序的;通过workflow中的topoSort函数实现
    private List<Edge> outEdges = new ArrayList<>();
    private List<Edge> inEdges = new ArrayList<>();

    //构造函数
    public Task(String name, double taskSize){
        this.name = name;
        this.taskSize = taskSize;
    }

    // The following two methods are only used during constructing a workflow
    //默认访问权限的类、类属变量及方法，包内的任何类（包括继承了此类的子类）都可以访问它，
    // 而对于包外的任何类都不能访问它（包括包外继承了此类的子类）。default重点突出包；
    //所以仅包内workflow访问此方法
    //default
    public void insertEdge(TEdges inOrOut,Edge e){
        if(inOrOut == TEdges.IN){
            if(e.getDestination()!=this)
                throw new RuntimeException();
            inEdges.add(e);
        }
        else if(inOrOut == TEdges.OUT){
            if(e.getSource()!=this)
                throw new RuntimeException();
            outEdges.add(e);
        }
    }

    //-------------------------------------getters&setters--------------------------------

    public String getName() {
        return name;
    }
    public double getTaskSize() {
        return taskSize;
    }
    public List<Edge> getOutEdges() {
        return outEdges==null ? null : Collections.unmodifiableList(outEdges);
    }
    public List<Edge> getInEdges() {
        return inEdges==null ? null : Collections.unmodifiableList(inEdges);
    }
    public VM_Private getPriavteVM() {
        return priavteVM;
    }
    public void setPriavteVM(VM_Private priavteVM) {
        this.priavteVM = priavteVM;
    }
    public VM_Public getPublicVM() {
        return publicVM;
    }
    public void setPublicVM(VM_Public publicVM) {
        this.publicVM = publicVM;
    }


    public void sortEdges(TEdges inOrOut, Comparator<Edge> comp){
        if(inOrOut == TEdges.IN){
            Collections.sort(inEdges,comp);
        }
        else{
            Collections.sort(outEdges,comp);
        }
    }

    public void setRunOnPrivateOrPublic(boolean runOnPrivateOrPublic) {
        RunOnPrivateOrPublic = runOnPrivateOrPublic;
    }

    public boolean getRunOnPrivateOrPublic(){
        return this.RunOnPrivateOrPublic;
    }

    @Override
    public String toString() {
        return "Task{" +
                "name='" + name + '\'' +
                ", taskSize=" + taskSize +
                '}';
    }

    public double getAST() {
        return AST;
    }

    public void setAST(double AST) {
        this.AST = AST;
    }

    public double getAFT() {
        return AFT;
    }

    public void setAFT(double AFT) {
        this.AFT = AFT;
    }

    public double getEST() {
        return EST;
    }

    public void setEST(double EST) {
        this.EST = EST;
    }

    public double getEFT() {
        return EFT;
    }

    public void setEFT(double EFT) {
        this.EFT = EFT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Task task = (Task) o;
        return  name.equals(task.getName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }



    //只用于PEFT算法,第一个元素存储私有云的OCT值，第二个元素存储公有云的OCT值；
    private double [] OCT = new double[2];
    public void setOCT(double privateVMOCT,double publicVMOCT){
        OCT[0] = privateVMOCT;
        OCT[1] = publicVMOCT;
    }
    public double[] getOCT() {
        return OCT;
    }

    //只用于IPPTS算法,第一个元素存储私有云的PCM值，第二个元素存储公有云的PCM值；
    private double [] PCM = new double[2];
    public void setPCM(double privateVMPCM,double publicVMPCM){
        PCM[0] = privateVMPCM;
        PCM[1] = publicVMPCM;
    }
    public double[] getPCM() {
        return PCM;
    }
    private double prank;
    public void setPrank(double prank){
        this.prank = prank;
    };
    public double getPrank() {
        return this.prank;}
    //只用于adptor
    private int adptorVMid ;


    private VM vm;
    //改造，如果是私有云，vmid = 0
    public void setAdaptorVM(){
        if(this.getRunOnPrivateOrPublic() == true){
            this.adptorVMid = 0;
        }
        else{
            this.adptorVMid = 1;
        }
    }

    public int getAdptorVMid() {
        return adptorVMid;
    }
    public void setVM(VM vm){
        this.vm = vm;
    }

    public VM getVM() {
        return vm;
    }

}
