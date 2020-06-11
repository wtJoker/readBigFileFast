package single;

/**
 * 既可以作为读取的buffer用，又可以作为结果Key用
 * endIndex维护着array的结尾指针，endIndex之后的数据视为无效数据
 * tempStartIndex、tempEndIndex维护着两个临时指针，表示截取的字符串
 */
public class BufferKey {
    private byte[] array;
    private int endIndex = -1;

    private int tempStartIndex = -1;
    private int tempEndIndex = -1;

    public BufferKey(byte[] array) {
        this.array = array;
        this.endIndex = array.length - 1;
        this.tempStartIndex = 0;
        this.tempEndIndex = endIndex;
    }

    public BufferKey(int length) {
        this.array = new byte[length];
    }

    public void add(byte b) {
        array[++endIndex] = b;
    }

    public void reset() {
        endIndex = -1;
        tempStartIndex = -1;
        tempEndIndex = -1;
    }

    public int getEndIndex() {
        return endIndex;
    }

    public byte[] getArray() {
        return array;
    }

    public void updateEndIndex(int endIndex) {
        this.endIndex = endIndex;
    }

    public void setTempStartIndex(int tempStartIndex) {
        this.tempStartIndex = tempStartIndex;
    }

    public void setTempEndIndex(int tempEndIndex) {
        this.tempEndIndex = tempEndIndex;
    }

    public byte[] getTempArray() {
        int length = tempEndIndex - tempStartIndex + 1;
        byte[] result = new byte[length];
        System.arraycopy(array, tempStartIndex, result, 0, length);
        return result;
    }

    @Override
    public String toString() {
        byte[] result = new byte[endIndex + 1];
        System.arraycopy(array, 0, result, 0, endIndex + 1);
        return new String(result);
    }

    @Override
    public int hashCode() {
        // warm : 在放入map时调用，用当前临时开始指针和临时结束指针之间的byte计算
        int h = 1;
        for (int i = tempStartIndex; i <= tempEndIndex; i++) {
            h = 31 * h + array[i];
        }
        return h;
    }

    @Override
    public boolean equals(Object obj) {
        // warm : 用当前临时开始指针和临时结束指针比较，此方法只在添加HashMap时作为key比较，那么这里传入对象只能是 BufferKey
        BufferKey bufferKey = (BufferKey) obj;
        if ((bufferKey.tempEndIndex - bufferKey.tempStartIndex) != (this.tempEndIndex - this.tempStartIndex)) {
            return false;
        }
        for (int i = 0, offset = tempEndIndex - tempStartIndex; i <= offset; i++) {
            if (bufferKey.array[i + bufferKey.tempStartIndex] != this.array[i + tempStartIndex]) {
                return false;
            }
        }
        return true;
    }
}
