package wolf.tfs;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class TInyFileStorageTest {

    static TinyFileStorage tfs = new TinyFileStorage.Builder("/tmp/tfs")
            .dataMaxSize(20 * 1024 * 1024)
            .build();

    @Test
    public void write() throws IOException {
        List<String> imgs = Arrays.asList(Paths.get("/usr/share/wallpapers/deepin").toFile().list());
        for (String img : imgs) {
            System.out.println(img + ":" + tfs.write(Paths.get("/home/myliang/Pictures", img)));
        }
        tfs.close();
    }

    @Test
    public void read() throws IOException {
        String[] urls = { "vwApONIo4PMY", "9vXl99IJlMg2", "VAM4D8Ie4qYz", "K2WlkzIXpZ0r", "bdV4jMIXaA7J" };
        for (String url : urls) {
            Files.write(Paths.get("/tmp/tfs/xx", url), tfs.read(url));
        }
        tfs.close();
    }

}
