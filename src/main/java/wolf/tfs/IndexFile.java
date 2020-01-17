package wolf.tfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class IndexFile {

    // 40M
    static int INIT_CAPACITY = 4 * 10 * 1024 * 1024;

    private Path fp;
    private OutputStream fos;
    private InputStream fis;

    private List<IndexNode> nodes = new ArrayList<>(INIT_CAPACITY);
    // 当前数据大小（包含没有写入数据文件的数据）
    private long size = 0;
    // 未同步到索引文件的的开始索引
    private int unSyncIndex = 0;

    private ReentrantLock lock = new ReentrantLock();
    private Condition condition = lock.newCondition();

    private IndexFile(String mount) {
        fp = Paths.get(mount, "idx");
        try {
            if (!Files.exists(fp)) {
                Files.createFile(fp);
            }
            fos = Files.newOutputStream(fp, StandardOpenOption.APPEND);
            fis = Files.newInputStream(fp);
        } catch (IOException e) {
            e.printStackTrace();
        }
        new Sync().start();
    }

    private IndexFile reload() {
        byte[] bytes = new byte[14];
        for (;;) {
            int read = 0;
            try {
                read = fis.read(bytes, 0, bytes.length);
                if (read < 0) break;
                if (read == bytes.length) {
                    nodes.add(IndexNode.decode(bytes));
                }
            } catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
        return this;
    }

    public IndexNode write(int fileSize) {
        lock.lock();
        try {
            byte fid = 0;
            IndexNode node = new IndexNode(fid, size, fileSize);
            nodes.add(node);
            size += fileSize;
            condition.signal();
            return node;
        } finally {
            lock.unlock();
        }
    }

    public static IndexFile load(String mount) {
        return new IndexFile(mount).reload();
    }

    class Sync extends Thread {
        @Override
        public void run() {
            while (true) {
                try {
                    condition.wait();
                    int size = nodes.size() - 1;
                    while (unSyncIndex < size) {
                        byte[] bytes = nodes.get(unSyncIndex).encode();
                        fos.write(bytes);
                        unSyncIndex++;
                    }
                    fos.flush();
                } catch (InterruptedException | IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
