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
import java.util.function.BiConsumer;

public class SingleThreadRWTest {

    static int readBufferSize = 4096 * 100;

    @Test
    public void read() throws IOException {
//        randAccessFileRead();
//        fileChannelRead();
//        mapRead();
    }

    @Test
    public void randAccessFileRead() throws IOException {
        eachReadFile((i, raf) -> {
            try {
                byte[] bytes = new byte[readBufferSize];
                raf.seek(i * bytes.length);
                raf.read(bytes, 0, readBufferSize);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void mapRead() throws IOException {
        eachReadFile((i, raf) -> {
            MappedByteBuffer mbb = null;
            try {
                byte[] bytes = new byte[readBufferSize];
                mbb = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, i + bytes.length, bytes.length);
                mbb.get(bytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    @Test
    public void fileChannelRead() throws IOException {
        eachReadFile((i, raf) -> {
            try {
                ByteBuffer buffer = ByteBuffer.allocate(readBufferSize);
                raf.getChannel().read(buffer);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public void eachReadFile(BiConsumer<Integer, RandomAccessFile> biConsumer) throws IOException {
        RandomAccessFile raf = new RandomAccessFile("/tmp/sraf.data", "rw");
        long start = System.currentTimeMillis();
        for (int i = 0; i < 100; i++) {
            biConsumer.accept(i, raf);
        }
        System.out.println("runtime: " + (System.currentTimeMillis() - start) + " ms in mapRead");
        raf.close();
    }

    @Test
    public void write() throws IOException {
        // fileChannelWrite();
        // randAccessFileWrite();
        // mapWrite();
        // 单线程写入channel 比 randAccessFile 强
    }

    @Test
    public void randAccessFileWrite() throws IOException {
        RandomAccessFile raf = new RandomAccessFile("/tmp/sraf.data", "rw");
        eachFiles("randAccessFileWrite", (offset, bytes) -> {
            try {
                 raf.write(bytes, 0, bytes.length);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        raf.close();
    }

    @Test
    public void fileChannelWrite() throws IOException {
        RandomAccessFile raf = new RandomAccessFile("/tmp/sfc.data", "rw");
        FileChannel fc = raf.getChannel();
        eachFiles("fileChannelWrite", (offset, bytes) -> {
            try {
                fc.write(ByteBuffer.wrap(bytes));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        fc.close();
        raf.close();
    }

    @Test
    public void mapWrite() throws IOException {
        RandomAccessFile raf = new RandomAccessFile("/tmp/sm.data", "rw");
        FileChannel fc = raf.getChannel();
        eachFiles("mapWrite", (offset, bytes) -> {
            try {
                MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_WRITE, offset, bytes.length);
                mbb.put(bytes);
//                mbb.force();
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

    public void eachFiles(String methodName, BiConsumer<Integer, byte[]> biConsumer) throws IOException {
        int totalMS = 0;
        int offset = 0;
        for (int i = 0; i < 100; i++) {
            for (byte[] bytes : fileBytes) {
                long start = System.currentTimeMillis();
                biConsumer.accept(offset, bytes);
                offset += bytes.length;
                totalMS += System.currentTimeMillis() - start;
            }
        }
        System.out.println("runtime: " + totalMS + " ms in " + methodName);
    }

}
