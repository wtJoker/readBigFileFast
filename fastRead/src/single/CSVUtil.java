package single;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CSV文件导出工具类
 */

public class CSVUtil {

    /**
     * CSV文件生成方法
     */
    public static File createCSVFile(List<Map.Entry<BufferKey, AtomicInteger>> dataList,
                                     String outPutPath, String filename, int topNum) {

        File csvFile = null;
        BufferedWriter csvWtriter = null;
        try {
            csvFile = new File(outPutPath + filename + ".csv");
            File parent = csvFile.getParentFile();
            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }

            if (parent != null && !parent.exists()) {
                parent.mkdirs();
            }
            csvFile.createNewFile();
            csvWtriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(csvFile)), 1024);

            // 写入文件内容
            writeRow(dataList, csvWtriter, topNum);
            csvWtriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (csvWtriter != null) {
                    csvWtriter.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return csvFile;
    }

    private static void writeRow(List<Map.Entry<BufferKey, AtomicInteger>> row, BufferedWriter csvWriter,
                                 int topNum) throws IOException {
        if (row == null || row.size() == 0) {
            return;
        }
        if (topNum < 0) {
            topNum = row.size();
        }
        for (int i = 0; i < topNum; i++) {
            Map.Entry<BufferKey, AtomicInteger> item = row.get(i);
            StringBuffer sb = new StringBuffer();
            String rowStr = sb.append("\"").append(item.getKey().toString()).append("\",")
                    .append(item.getValue().get()).toString();
            csvWriter.write(rowStr);
            csvWriter.newLine();
        }
    }

}

