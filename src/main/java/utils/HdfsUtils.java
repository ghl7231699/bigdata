package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * hdfs在java 的方法（创建文件夹 上传文件 下载文件 删除文件）
 *
 * @author hadoop
 */
public class HdfsUtils {
    public static final String HDFS_PATH = "hdfs://ghl02:8020/user/ghl/mapreduce/output";
    public static final String DIR_PATH = "hdfs://ghl02:8020/user/ghl/mapreduce/output";
    public static final String FILE_PATH = "hdfs://ghl02:8020/user/ghl/mapreduce/output";

    public static void main(String[] args) throws Exception {
        final FileSystem fileSystem = FileSystem.get(new URI(HDFS_PATH),
                new Configuration());
        // 创建文件夹
//       makeDirectory(fileSystem);
        // 上传文件
//       uploadData(fileSystem);
        // 下载文件
//       downloadData(fileSystem);
        // 删除文件
//        deleteFile(fileSystem);

    }

    /**
     * 删除文件夹
     *
     * @param conf
     * @param filePath
     * @throws IOException
     */
    public static boolean deleteFile(Configuration conf, String filePath)
            throws IOException {
        // 删除文件-true/false（文件夹-true）
        Path file = new Path(filePath);
        FileSystem fileSystem = file.getFileSystem(conf);
        if (fileSystem.exists(file)) {
            fileSystem.delete(file, true);
        }
        return fileSystem.exists(file);
    }

    private static void downloadData(final FileSystem fileSystem)
            throws IOException {
        final FSDataInputStream in = fileSystem.open(new Path(FILE_PATH));
        IOUtils.copyBytes(in, System.out, 1024, true);
    }

    private static void makeDirectory(final FileSystem fileSystem)
            throws IOException {
        // 创建文件夹
        fileSystem.mkdirs(new Path(DIR_PATH));
    }

    private static void uploadData(final FileSystem fileSystem)
            throws IOException, FileNotFoundException {
        // 上传文件
        final FSDataOutputStream out = fileSystem.create(new Path(FILE_PATH)); //上传后的文件命令名
        FileInputStream in = new FileInputStream("./hello.txt"); //上传的文件
        IOUtils.copyBytes(in, out, 1024, true);
    }

    /**
     * 输出指定文件内容
     *
     * @param conf     HDFS配置
     * @param filePath 文件路径
     * @return 文件内容
     * @throws IOException
     */
    public static void cat(Configuration conf, String filePath) throws IOException {

        InputStream in = null;
        Path file = new Path(filePath);
        FileSystem fileSystem = file.getFileSystem(conf);
        try {
            in = fileSystem.open(file);
            IOUtils.copyBytes(in, System.out, 4096, true);
        } finally {
            if (in != null) {
                IOUtils.closeStream(in);
            }
        }
    }
}
