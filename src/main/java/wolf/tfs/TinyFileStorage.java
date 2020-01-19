package wolf.tfs;

import org.hashids.Hashids;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TinyFileStorage {

    private Hashids hashids;
    private Builder builder;
    private IndexNodes indexNodes;

    private short dataFileId = -1;
    private FileChannel dataWriteChannel;
    private Map<Short, FileChannel> dataReadChannels = new HashMap<>();

    private FileChannel indexWriteChannel;

    public TinyFileStorage(Builder builder) {
        this.builder = builder;
        this.hashids = new Hashids(builder.salt, 12);
        this.indexNodes = new IndexNodes(builder.dataMaxSize, builder.averageSize);

        Path idx = builder.indexFilePath();
        try {
            if (!Files.exists(idx)) {
                Files.createFile(idx);
            }
            indexWriteChannel = FileChannel.open(idx, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            InputStream is = Files.newInputStream(idx);
            byte[] bytes = new byte[IndexNode.BYTE_SIZE];
            for (;;) {
                int read = 0;
                try {
                    read = is.read(bytes, 0, bytes.length);
                    if (read < 0) break;
                    if (read == bytes.length) {
                        indexNodes.add(bytes);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String write(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);

        synchronized (this) {
            // first
            IndexNode node = indexNodes.add(bytes.length);

            // second: write the data file
            FileChannel dfc = getDataWriteChannel();
            dfc.write(ByteBuffer.wrap(bytes));

            // write index to file
            indexWriteChannel.write(node.encode());

            return hashids.encode(indexNodes.fid(), indexNodes.nid());
        }
    }

    public byte[] read(String url) throws IOException {
        long[] ids = hashids.decode(url);
        short fid = (short) ids[0];
        int nid = (int) ids[1];
        IndexNode node = indexNodes.get(fid, nid);
        if (node == null) throw new RuntimeException("url error!");

        byte[] bytes = new byte[node.getSize()];
        FileChannel dfc = getDataReadChannel(fid);
        MappedByteBuffer mbb = dfc.map(FileChannel.MapMode.READ_ONLY, node.getOffset(), node.getSize());
        mbb.get(bytes);
        return bytes;
    }

    public void close() {
        this.closeIndexWriteChannel();
        this.closeDataWriteChannel();
        dataReadChannels.forEach((fid, it) -> {
            try {
                it.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    private void closeIndexWriteChannel() {
        try {
            indexWriteChannel.force(true);
            indexWriteChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void closeDataWriteChannel() {
        try {
            if (dataWriteChannel != null) {
                dataWriteChannel.force(true);
                dataWriteChannel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private synchronized FileChannel getDataWriteChannel() throws IOException {
        short fid = indexNodes.fid();
        if (dataFileId < fid) {
            if (dataWriteChannel != null) {
                closeDataWriteChannel();
            }
            dataWriteChannel = FileChannel.open(builder.dataFilePath(fid),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            dataFileId = fid;
        }
        return dataWriteChannel;
    }

    private FileChannel getDataReadChannel(short fid) throws IOException {
        if (!dataReadChannels.containsKey(fid)) {
            synchronized (this) {
                if (!dataReadChannels.containsKey(fid)) {
                    dataReadChannels.put(fid, FileChannel.open(builder.dataFilePath(fid),
                            StandardOpenOption.READ));
                }
            }
        }
        return dataReadChannels.get(fid);
    }

    public static final class Builder {
        // the file storage directory
        String mount;
        String dataFileNamePrefix;
        // the file size: byte
        long dataMaxSize;
        // byte
        int averageSize;
        String salt;

        public Builder(String mount) {
            this.mount = mount;
            this.dataFileNamePrefix = "";
            this.dataMaxSize = 100L * Constant.GB; // 100G
            this.averageSize = Constant.MB; // 1M
            this.salt = "wolf-tiny-file-storage";
        }

        public Builder dataMaxSize(long size) {
            this.dataMaxSize = size;
            return this;
        }

        public Builder averageSize(int size) {
            this.averageSize = size;
            return this;
        }

        public Builder dataFileNamePrefix(String prefix) {
            this.dataFileNamePrefix = prefix;
            return this;
        }

        public Builder salt(String salt) {
            this.salt = salt;
            return this;
        }

        public Path indexFilePath() {
            return Paths.get(mount, "_.tx");
        }
        public Path dataFilePath(short id) {
            return Paths.get(mount, dataFileNamePrefix + "_" + id + ".td");
        }

        public TinyFileStorage build() {
            return new TinyFileStorage(this);
        }
    }
}
