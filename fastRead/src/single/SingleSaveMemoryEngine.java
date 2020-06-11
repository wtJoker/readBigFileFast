package single;

public class SingleSaveMemoryEngine {
    public static void main(String[] args) {
        long begin = System.currentTimeMillis();
        String outPutPath = "";
        String filePath = args[0];
        String name = args[1];
        String domainFileName = name + "_domain";
        String urlFileName = name + "_uri";
        SingleThreadReader.launch(filePath, outPutPath, domainFileName, urlFileName,
                10, 6, 19, 32);
        System.out.println(System.currentTimeMillis() - begin);
    }
}
