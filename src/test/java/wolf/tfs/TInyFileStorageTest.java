package wolf.tfs;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class TInyFileStorageTest {

    static TinyFileStorage tfs = new TinyFileStorage.Builder("/tmp/tfs").build();

    @Test
    public void write() throws IOException {
        List<String> imgs = Arrays.asList(Paths.get("/usr/share/wallpapers/deepin").toFile().list());
        for (String img : imgs) {
            System.out.println(tfs.write(Paths.get("/home/myliang/Pictures", img)));
        }
    }

    @Test
    public void read() throws IOException {
        String[] urls = { "vwApOwTmpPMY", "edjlwbT6pmQW", "kv74dbTylQPO", "90d4gXTnpJ3Z", "5Bj4VZT94w36" };
        for (String url : urls) {
            Files.write(Paths.get("/tmp/tfs/xx", url), tfs.read(url));
        }
    }

}
