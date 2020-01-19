### Tiny File Storage() for Java

#### demo
```java
   static TinyFileStorage tfs = new TinyFileStorage.Builder("/tmp/tfs").build();
   // write
   String url = tfs.write(Paths.get("/tmp/Pictures", img))
   // read
   Files.write(Paths.get("/tmp/tfs/xx", url), tfs.read(url));
```
