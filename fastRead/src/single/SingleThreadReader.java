package single;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleThreadReader {

    /**
     * \n
     */
    private static final byte NEW_LINE_SYMBOL = 10;

    /**
     * 统计的目标文件
     */
    private File file;
    /**
     * 读取文件时的buffer大小
     */
    private int bufferSize;
    /**
     * 字符拆解分析统计线程池
     */
    private ExecutorService executorService;
    /**
     * 复用buffer池
     */
    private LinkedBlockingQueue<BufferKey> bufferPool;
    /**
     * domain统计结果输出文件名
     */
    private String domainFilename;
    /**
     * uri统计结果输出文件名
     */
    private String uriFilename;
    /**
     * 结果输出目录
     */
    private String outPutPath;
    /**
     * 结果输出top N
     */
    private int topNum;
    /**
     * 线程domain结果集合
     */
    private List<Map<BufferKey, AtomicInteger>> domainResultList;
    /**
     * 线程uri结果集合
     */
    private List<Map<BufferKey, AtomicInteger>> uriResultList;

    /**
     * domainThreadLocal
     */
    private static final ThreadLocal<Map<BufferKey, AtomicInteger>> domainThreadLocal = new ThreadLocal<>();
    /**
     * uriThreadLocal
     */
    private static final ThreadLocal<Map<BufferKey, AtomicInteger>> uriThreadLocal = new ThreadLocal<>();

    /**
     * constructor
     *
     * @param file           统计的目标文件
     * @param outPutPath     结果输出目录
     * @param domainFilename domain统计结果输出文件名
     * @param uriFilename    uri统计结果输出文件名
     * @param topNum         结果输出top N
     * @param threadPoolSize 字符拆解分析统计线程池大小
     * @param bufferSize     读取文件时的buffer大小
     * @param bufferPoolSize buffer池大小
     */
    public SingleThreadReader(File file, String outPutPath, String domainFilename, String uriFilename,
                              int topNum, int threadPoolSize, int bufferSize, int bufferPoolSize) {
        this.file = file;
        this.outPutPath = outPutPath;
        this.domainFilename = domainFilename;
        this.uriFilename = uriFilename;
        this.topNum = topNum;
        this.bufferSize = bufferSize;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        this.domainResultList = new CopyOnWriteArrayList<>();
        this.uriResultList = new CopyOnWriteArrayList<>();
        bufferPool = new LinkedBlockingQueue<>(bufferPoolSize);
        for (int i = 0; i < bufferPoolSize; i++) {
            bufferPool.add(new BufferKey(this.bufferSize));
        }
    }

    public void start() {
        try {
            // 读取
            readByBuffer();
            // 安全关闭线程池
            executorService.shutdown();
            // 等待积压在线程池队列内的任务结束
            while (!executorService.isTerminated()) {
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            AnalysisHandle.mergeResultAndPostHandle(domainResultList, outPutPath, domainFilename, topNum);
            AnalysisHandle.mergeResultAndPostHandle(uriResultList, outPutPath, uriFilename, topNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取
     *
     * @throws Exception
     */
    private void readByBuffer() throws Exception {
        FileInputStream fis = new FileInputStream(this.file);
        BufferedInputStream bis = new BufferedInputStream(fis);
        // 实际读到的字节数，buffer数组开始读取偏移量，读取长度
        int cnt, offset, length;
        BufferKey buffer0 = bufferPool.take();
        for (; ; ) {
            byte[] arrayBuffer0 = buffer0.getArray();
            offset = buffer0.getEndIndex() + 1;
            length = bufferSize - buffer0.getEndIndex() - 1;
            cnt = bis.read(arrayBuffer0, offset, length);
            if (cnt == -1) break;
            int realEndIndex = buffer0.getEndIndex() + cnt;
            int newLineSymbolIndex = realEndIndex;
            for (; newLineSymbolIndex >= 0; newLineSymbolIndex--)
                if (arrayBuffer0[newLineSymbolIndex] == NEW_LINE_SYMBOL) break;
            if (newLineSymbolIndex == -1) {
                executorService.shutdown();
                bis.close();
                throw new Exception("请加大buffer size，一个buffer不能读完一整行！！");
            }
            buffer0.updateEndIndex(newLineSymbolIndex);

            BufferKey buffer1 = bufferPool.take();
            byte[] arrayBuffer1 = buffer1.getArray();
            int leftLength = realEndIndex - newLineSymbolIndex;
            System.arraycopy(arrayBuffer0, newLineSymbolIndex + 1, arrayBuffer1, 0, leftLength);
            buffer1.updateEndIndex(leftLength - 1);
            executorService.submit(new SliceReaderTask(bufferPool, buffer0));
            buffer0 = buffer1;
        }
        // 读到文件尾如果剩余半行buffer0不是空的，如果文件完整（有\n结尾）buffer0肯定是空的
        if (buffer0.getEndIndex() != -1) {
            buffer0.add(NEW_LINE_SYMBOL);
            executorService.submit(new SliceReaderTask(bufferPool, buffer0));
        }
        bis.close();
    }

    private class SliceReaderTask implements Runnable {
        private BufferKey buffer;
        private LinkedBlockingQueue<BufferKey> bufferPool;

        public SliceReaderTask(LinkedBlockingQueue<BufferKey> bufferPool, BufferKey buffer) {
            this.bufferPool = bufferPool;
            this.buffer = buffer;
        }

        private Map<BufferKey, AtomicInteger> domainMap;
        private Map<BufferKey, AtomicInteger> uriMap;

        private void prepare() {
            domainMap = domainThreadLocal.get();
            uriMap = uriThreadLocal.get();
            if (domainMap == null) {
                domainMap = new HashMap<>();
                domainThreadLocal.set(domainMap);
                domainResultList.add(domainMap);
            }
            if (uriMap == null) {
                uriMap = new HashMap<>();
                uriThreadLocal.set(uriMap);
                uriResultList.add(uriMap);
            }
        }

        @Override
        public void run() {
            try {
                prepare();
                AnalysisHandle.analysis(buffer, domainMap, uriMap);
                buffer.reset();
                // put 添加如果满了会阻塞，offer 队列满返回false，这里不可能满, add 添加如果满了会抛 IllegalStateException 异常
                bufferPool.put(buffer);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void launch(String filePath, String outPutPath, String domainFileName, String urlFileName,
                              int topNum, int threadPoolSize, int bufferSizePower, int bufferPoolSize) {
        new SingleThreadReader(
                new File(filePath), outPutPath, domainFileName, urlFileName,
                topNum, threadPoolSize, 1 << bufferSizePower, bufferPoolSize
        ).start();
    }
}

