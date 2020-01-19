package wolf.tfs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IndexNodes {

    private Map<Short, List<IndexNode>> dataNodesMap = new ConcurrentHashMap();

    private int initCapacity;

    // 每个数据文件的最大值
    private long dataMaxSize;

    // 当前数据文件的id
    private short dataFileId = 0;

    // 当前数据文件大小
    private long dataSize = 0;

    public IndexNodes(long dataMaxSize, int averageSize) {
        this.dataMaxSize = dataMaxSize;
        this.initCapacity = (int) dataMaxSize / averageSize;
    }

    public IndexNode add(int byteSize) {
        return add(new IndexNode(dataFileId, dataSize, byteSize));
    }

    public IndexNode add(byte[] bytes) {
        return add(IndexNode.decode(bytes));
    }

    public IndexNode add(IndexNode node) {
        if (dataSize > dataMaxSize) {
            dataFileId++;
            dataSize = 0;
        }
        gets().add(node);
        dataSize += node.getSize();
        return node;
    }

    public List<IndexNode> gets() {
        return gets(dataFileId);
    }

    public IndexNode get(short fid, int nid) {
        List<IndexNode> nodes = gets(fid);
        if (nodes != null && nodes.size() > nid) {
            return nodes.get(nid);
        }
        return null;
    }

    public List<IndexNode> gets(short fid) {
        if (!dataNodesMap.containsKey(fid)) {
            dataNodesMap.put(fid, new ArrayList<>(initCapacity));
        }
        return dataNodesMap.get(fid);
    }

    public short fid() {
        return dataFileId;
    }

    public int nid() {
        return gets().size() - 1;
    }
}
