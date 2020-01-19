package wolf.tfs;

import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class RandAccessFileTest {

    // desktop.jpg size: 8.8M

    // 默认写入的bufSize: 2048
    // it will be a about 6ms running
    @Test
    public void writeByteSizeOnce() throws IOException {
        for (String fileName : Paths.get("/usr/share/wallpapers/deepin/").toFile().list()) {
            byte[] bytes = Files.readAllBytes(Paths.get("/usr/share/wallpapers/deepin/", fileName));
            long start = System.currentTimeMillis();
            RandomAccessFile raf = new RandomAccessFile("/tmp/" + fileName, "rw");
            raf.write(bytes, 0, bytes.length);
            raf.close();
            System.out.println("runtime: " + (System.currentTimeMillis() - start) + " ms, bytes: " + bytes.length);
        }
    }

    // 512: 19ms
    // 1024: 12ms
    // 2048: 6ms
    // 4096: 4ms
    // 8192: 4ms
    // 10000: 4ms
    @Test
    public void writeByteSizeMany() throws IOException {
//        byte[] bytes = Files.readAllBytes(Paths.get("/usr/share/wallpapers/deepin/desktop.jpg"));
        for (String fileName : Paths.get("/usr/share/wallpapers/deepin/").toFile().list()) {
            byte[] bytes = Files.readAllBytes(Paths.get("/usr/share/wallpapers/deepin/", fileName));
            long startm = System.currentTimeMillis();
            RandomAccessFile raf = new RandomAccessFile("/tmp/" + fileName, "rw");
            raf.seek(0);
            int bufSize = 4096;
            for (int i = 0; i < bytes.length; i += bufSize) {
                int bsize = bufSize;
                int leftSize = bytes.length - i;
                if (bsize > leftSize) bsize = leftSize;
                raf.write(bytes, i, bsize);
            }
            raf.close();
            System.out.println("runtime: " + (System.currentTimeMillis() - startm) + " ms, bytes: " + bytes.length);
        }
    }

    // 13ms
    @Test
    public void Files_readAllBytes() throws IOException {
        long start = System.currentTimeMillis();
        Files.readAllBytes(Paths.get("/tmp/desktop.jpg"));
        System.out.println("runtime: " + (System.currentTimeMillis() - start) + " ms");
    }

    // 1024: 9ms
    // 2048: 8ms
    // 4096: 6ms
    // 8192: 6ms
    @Test
    public void readByteSize() throws IOException {
        long start = System.currentTimeMillis();
        RandomAccessFile raf = new RandomAccessFile("/tmp/desktop.jpg", "r");
        System.out.println("runtime: " + (System.currentTimeMillis() - start) + " ms");
        start = System.currentTimeMillis();
        int bufSize = 4096;
        int fsize = (int) raf.getChannel().size();
        byte[] bytes = new byte[fsize];
        int r = 0;
        while (true) {
            r = raf.read(bytes, r, bufSize);
            if (r < bufSize) {
                break;
            }
        }
        raf.close();
        System.out.println("runtime: " + (System.currentTimeMillis() - start) + " ms");
    }

    @Test
    public void readByteSizeByMap() throws IOException {
        long start = System.currentTimeMillis();
        // FileChannel fc = FileChannel.open(Paths.get("/tmp/desktop.jpg"), StandardOpenOption.READ);
        RandomAccessFile raf = new RandomAccessFile("/tmp/desktop.jpg", "r");
        FileChannel fc = raf.getChannel();
        System.out.println("runtime: " + (System.currentTimeMillis() - start) + " ms");
        start = System.currentTimeMillis();
        long size = fc.size();
        MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0, size);
        byte[] bytes = new byte[(int)size];
        mbb.get(bytes);
        fc.close();
        System.out.println("runtime: " + (System.currentTimeMillis() - start) + " ms");
    }
}
