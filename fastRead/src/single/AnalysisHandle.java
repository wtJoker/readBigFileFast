package single;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 丢进来的buffer保证是整行，即\n结尾
 */
public class AnalysisHandle {

    /**
     * \t
     */
    private static final byte TAB_SYMBOL = 9;
    /**
     * \n
     */
    private static final byte NEW_LINE_SYMBOL = 10;
    /**
     * space
     */
    private static final byte SPACE_SYMBOL = 32;
    /**
     * ?
     */
    private static final byte QUESTION_SYMBOL = 63;

    /**
     * 分割出domain和uri并添加进每个线程的统计map中
     *
     * @param buffer    buffer
     * @param domainMap domain结果集合
     * @param uriMap    uriMap结果集合
     */
    public static void analysis(BufferKey buffer,
                                Map<BufferKey, AtomicInteger> domainMap,
                                Map<BufferKey, AtomicInteger> uriMap) {
        int index = 0;
        byte[] readBuff = buffer.getArray();
        int endIndex = buffer.getEndIndex();

        int domainStartIndex;
        int uriStartIndex;
        int spaceCount;
        for (; ; ) {
            // 跳过前三个tab
            for (; ; ) if (TAB_SYMBOL == readBuff[index++]) break;
            for (; ; ) if (TAB_SYMBOL == readBuff[index++]) break;
            for (; ; ) if (TAB_SYMBOL == readBuff[index++]) break;

            domainStartIndex = index;
            uriStartIndex = 0;
            spaceCount = 0;
            // 读取内容
            for (; ; ) {
                byte b = readBuff[index++];
                if (TAB_SYMBOL == b) break;
                if (SPACE_SYMBOL == b) {
                    spaceCount++;
                    if (spaceCount == 1) {
                        increment(buffer, domainStartIndex, index - 2, domainMap);
                    } else if (spaceCount == 5) {
                        uriStartIndex = index;
                    } else if (spaceCount == 6) {
                        increment(buffer, uriStartIndex, index - 2, uriMap);
                        break;
                    }
                }
                if (spaceCount == 5 && QUESTION_SYMBOL == b) {
                    increment(buffer, uriStartIndex, index - 2, uriMap);
                    break;
                }
            }

            // 读到行尾部
            for (; ; ) if (NEW_LINE_SYMBOL == readBuff[index++]) break;
            // 读到buffer尾部
            if (index >= endIndex) break;
        }
    }

    /**
     * @param buffer buffer
     * @param start  在buffer中的起始位置
     * @param end    在buffer中的结束位置
     * @param map    结果集合
     */
    private static void increment(BufferKey buffer, int start, int end, Map<BufferKey, AtomicInteger> map) {
        buffer.setTempStartIndex(start);
        buffer.setTempEndIndex(end);
        if (map.containsKey(buffer)) {
            map.get(buffer).incrementAndGet();
        } else {
            BufferKey key = new BufferKey(buffer.getTempArray());
            map.put(key, new AtomicInteger(1));
        }
    }

    /**
     * 合并每个线程统计出来的结果、排序、打印结果
     *
     * @param resultList 每个线程统计出来的结果
     * @param outPutPath 输出路径
     * @param fileName   输出文件名
     * @param topNum     输出top N
     */
    public static void mergeResultAndPostHandle(Collection<Map<BufferKey, AtomicInteger>> resultList,
                                                String outPutPath, String fileName, int topNum) {
        postHandle(mergeResult(resultList), outPutPath, fileName, topNum);
    }

    /**
     * 合并每个线程统计出来的结果
     *
     * @param resultList 所有结果集合
     * @return 结果集合
     */
    private static Map<BufferKey, AtomicInteger> mergeResult(Collection<Map<BufferKey, AtomicInteger>> resultList) {
        Map<BufferKey, AtomicInteger> map = new HashMap<>();
        for (Map<BufferKey, AtomicInteger> item : resultList) {
            for (Map.Entry<BufferKey, AtomicInteger> entry : item.entrySet()) {
                AtomicInteger atomicInteger = map.get(entry.getKey());
                if (atomicInteger == null) {
                    map.put(entry.getKey(), entry.getValue());
                } else {
                    atomicInteger.addAndGet(entry.getValue().get());
                }
            }
        }
        return map;
    }

    /**
     * 后处理：转换成有序List、排序、打印结果
     *
     * @param map        结果集合
     * @param outPutPath 输出路径
     * @param fileName   输出文件名
     * @param topNum     输出top N
     */
    public static void postHandle(Map<BufferKey, AtomicInteger> map, String outPutPath, String fileName, int topNum) {
        // 转换成有序List
        List<Map.Entry<BufferKey, AtomicInteger>> domainList = new ArrayList<>(map.entrySet());
        // 排序
        domainList.sort((e1, e2) -> -Integer.compare(e1.getValue().get(), e2.getValue().get()));
        // 打印结果
        CSVUtil.createCSVFile(domainList, outPutPath, fileName, topNum);
    }


}
