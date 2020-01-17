package wolf.tfs;

import org.hashids.Hashids;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.rmi.server.ExportException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TinyFileStorage {

    static final int MB = 1024 * 1024;
    static final int GB = 1024 * MB;
    // 40M
    static final int INIT_CAPACITY = 4 * 10 * MB;

    private Hashids hashids;
    private Builder builder;
    private List<IndexNode> nodes = new ArrayList<>(INIT_CAPACITY);

    // 当前数据文件的大小
    private long dataSize = 0;
    // 文件id
    private byte dataFileId = 0;
    private List<OutputStream> dataOutputs = new ArrayList<>();

    private OutputStream ios;

    public TinyFileStorage(Builder builder) {
        this.builder = builder;
        this.hashids = new Hashids(builder.salt, 12);
        Path idx = builder.indexFilePath();
        try {
            if (!Files.exists(idx)) {
                Files.createFile(idx);
            }
            ios = Files.newOutputStream(idx, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            InputStream is = Files.newInputStream(idx);
            byte[] bytes = new byte[IndexNode.BYTE_SIZE];
            for (;;) {
                int read = 0;
                try {
                    read = is.read(bytes, 0, bytes.length);
                    if (read < 0) break;
                    if (read == bytes.length) {
                        IndexNode node = IndexNode.decode(bytes);
                        nodes.add(node);
                        dataSize += node.getSize();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    break;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String write(Path path) throws IOException {
        byte[] bytes = Files.readAllBytes(path);

        synchronized (this) {
            // cal dataFileId
            if (dataSize > builder.dataMaxSize) {
                dataFileId++;
            }

            // write the data file
            OutputStream dos = getDataOutputStream();
            dos.write(bytes, 0, bytes.length);
            dos.flush();

            // update nodes & the idx file
            IndexNode node = new IndexNode(dataFileId, dataSize, bytes.length);
            nodes.add(node);
            ios.write(node.encode());
            ios.flush();

            dataSize += bytes.length;
            return hashids.encode(dataFileId, nodes.size() - 1);
        }
    }

    public byte[] read(String url) throws IOException {
        long[] ids = hashids.decode(url);
        short fid = (short) ids[0];
        int nid = (int) ids[1];
        if (nodes.contains(nid)) throw new RuntimeException("url error!");
        IndexNode node = nodes.get(nid);
        byte[] bytes = new byte[node.getSize()];

        RandomAccessFile rcf = new RandomAccessFile(builder.dataFilePath(fid).toFile(), "r");
        rcf.seek(node.getOffset());
        rcf.read(bytes, 0, node.getSize());
        rcf.close();
        return bytes;
    }

    private OutputStream getDataOutputStream() throws IOException {
        if (!dataOutputs.contains(dataFileId)) {
            dataOutputs.add(Files.newOutputStream(builder.dataFilePath(dataFileId), StandardOpenOption.CREATE, StandardOpenOption.APPEND));
        }
        return dataOutputs.get(dataFileId);
    }

    public static final class Builder {
        // the file storage directory
        String mount;
        String dataFileNamePrefix;
        // the file size: Gb
        long dataMaxSize;
        String salt;

        public Builder(String mount) {
            this.mount = mount;
            this.dataFileNamePrefix = "_";
            this.dataMaxSize = 100L * GB; // 100G
            this.salt = "wolf-tiny-file-storage";
        }

        public Builder dataMaxSize(int size) {
            this.dataMaxSize = size;
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
            return Paths.get(mount, dataFileNamePrefix + id + ".td");
        }

        public TinyFileStorage build() {
            return new TinyFileStorage(this);
        }
    }
}
