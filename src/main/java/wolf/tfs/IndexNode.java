package wolf.tfs;

import java.nio.ByteBuffer;

public class IndexNode {

    public static int BYTE_SIZE = 16;

    // data.{fid} data.0, data.1....
    private short fid;

    // the offset of file storing data
    private long offset;

    // the file size
    private int size;

    private short state;

    public IndexNode() {}

    public IndexNode(short fid, long offset, int size) {
        this.fid = fid;
        this.offset = offset;
        this.size = size;
        this.state = 0;
    }

    public ByteBuffer encode() {
        ByteBuffer buffer = ByteBuffer.allocate(BYTE_SIZE);
        buffer.putShort(fid);
        buffer.putLong(offset);
        buffer.putInt(size);
        buffer.putShort(state);
        buffer.flip();
        return buffer;
    }

    public static IndexNode decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        IndexNode node = new IndexNode();
        node.fid = buffer.getShort();
        node.offset = buffer.getLong();
        node.size = buffer.getInt();
        node.state = buffer.getShort();
        return node;
    }

    public short getFid() {
        return fid;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    public short getState() {
        return state;
    }
}
