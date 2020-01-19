package wolf.tfs;

import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;

public class MultiThreadRWTest {

    @Test
    public void read() {
        randAccessFileRead();
        fileChannelRead();
        mapRead();
    }

    public void randAccessFileRead() {}
    public void fileChannelRead() {}
    public void mapRead() {}

    @Test
    public void write() {
    }

    public void randAccessFileWrite() {}

    @Test
    public void fileChannelWrite() throws IOException, ExecutionException, InterruptedException {
        eachWriteFiles("fileChannelWrite", (offset, bytes) -> {
            try {
                RandomAccessFile raf = new RandomAccessFile("/tmp/mfc.data", "rw");
                FileChannel fc = raf.getChannel();
                fc.position(offset);
                fc.write(ByteBuffer.wrap(bytes));
                fc.close();
                raf.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

    }

    @Test
    public void mapWrite() throws IOException, ExecutionException, InterruptedException {
        RandomAccessFile raf = new RandomAccessFile("/tmp/mm.data", "rw");
        FileChannel fc = raf.getChannel();
        eachWriteFiles("mapWrite", (offset, bytes) -> {
            try {
                MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, offset, bytes.length);
                mbb.put(bytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        fc.close();
        raf.close();
    }

    static List<byte[]> fileBytes = new ArrayList<>();
    static {
        for (String fileName : Paths.get("/usr/share/wallpapers/deepin/").toFile().list()) {
            try {
                fileBytes.add(Files.readAllBytes(Paths.get("/usr/share/wallpapers/deepin/", fileName)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void eachWriteFiles(String methodName, BiConsumer<Integer, byte[]> biConsumer) throws InterruptedException, ExecutionException {
        ExecutorService es = Executors.newFixedThreadPool(fileBytes.size());
        int cnt = fileBytes.size() * 10;
        CountDownLatch latch = new CountDownLatch(cnt);

        long start = System.currentTimeMillis();
        final int[] offset = {0};
        for (int i = 0; i < 10; i++) {
            int idx = 0;
            for (byte[] bytes : fileBytes) {
                System.out.println("idx:" + idx++);
                es.execute(() -> {
                    try {
                        biConsumer.accept(offset[0], bytes);
                    } finally {
                        System.out.println("子线程："+Thread.currentThread().getName()+"执行," + latch.getCount());
                        latch.countDown();
                    }
                });
                offset[0] += bytes.length;
            }
        }
        System.out.println("等待线程执行完毕…… ……");
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("runtime: " + (System.currentTimeMillis() - start) + " ms in " + methodName);
    }

}
