package util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.net.URI;


public class HdfsClient {
    private static final Logger LOG = LoggerFactory.getLogger( HdfsClient.class);
    private static String hdfsURL = "hdfs://slave033:9000";

    /**
     * 获取文件系统
     *
     * @return FileSystem 文件系统
     */
    public static FileSystem getFileSystem() throws IOException {
        //读取配置文件
        // ====== Init HDFS File System Object
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", hdfsURL);
        // Because of Maven
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        // Set HADOOP user
        System.setProperty("HADOOP_USER_NAME", "experiment");
        System.setProperty("hadoop.home.dir", "/");
        //Get the filesystem - HDFS

        FileSystem fs = FileSystem.get(URI.create(hdfsURL), conf);
        return fs;
    }
    /**
     * 创建文件目录
     *
     * @param path 文件路径
     */
    public static void mkdir(String path) {
        try {
            FileSystem fs = getFileSystem();
            System.out.println("FilePath="+path);
            // 创建目录
            fs.mkdirs(new Path(path));
            //释放资源
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断目录是否存在
     *
     * @param filePath 目录路径
     * @param create 若不存在是否创建
     */
    public static  boolean existDir(String filePath, boolean create){
        boolean flag = false;

        if (StringUtils.isEmpty(filePath)){
            return flag;
        }

        try{
            Path path = new Path(filePath);
            // FileSystem对象
            FileSystem fs = getFileSystem();

            if (create){
                if (!fs.exists(path)){
                    fs.mkdirs(path);
                }
            }

            if (fs.isDirectory(path)){
                flag = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return flag;
    }

    /**
     * 本地文件上传至 HDFS
     *（hdfs没办法直接创建文件，需要本地创建然后上传）
     * @param srcFile 源文件 路径
     * @param destPath hdfs路径
     */
    public static void copyFileToHDFS(String srcFile,String destPath)throws Exception{

        FileInputStream fis=new FileInputStream(new File(srcFile));//读取本地文件
        FileSystem fs = getFileSystem();
        OutputStream os = fs.create(new Path(destPath));
        //copy
        IOUtils.copyBytes(fis, os, 4096, true);
        System.out.println("拷贝完成...");
        fs.close();
    }

    /**
     * 从 HDFS 下载文件到本地
     *
     * @param srcFile HDFS文件路径
     * @param destPath 本地路径
     */
    public static void getFile(String srcFile,String destPath)throws Exception {
        //hdfs文件 地址
        String file = hdfsURL+srcFile;
        Configuration config = new Configuration();
        //构建FileSystem
        FileSystem fs = FileSystem.get(URI.create(file),config);
        //读取文件
        InputStream is=fs.open(new Path(file));
        IOUtils.copyBytes(is, new FileOutputStream(new File(destPath)),2048, true);//保存到本地  最后 关闭输入输出流
        System.out.println("下载完成...");
        fs.close();
    }

    /**
     * 删除文件或者文件目录
     *
     * @param path
     */
    public static void rmdir(String path) {
        try {
            // 返回FileSystem对象
            FileSystem fs = getFileSystem();

            String hdfsUri = hdfsURL;
            if(StringUtils.isNotBlank(hdfsUri)){
                path = hdfsUri + path;
            }
            System.out.println("path:"+path);
            // 删除文件或者文件目录  delete(Path f) 此方法已经弃用
            System.out.println( fs.delete(new Path(path),true));

            // 释放资源
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     * 读取文件大小
     * @param filePath
     * @throws IOException
     */
    public static long readFileSize(String filePath) throws IOException{
        //Get the filesystem - HDFS
        FileSystem fs = getFileSystem();
        String file = filePath;
        LOG.info("文件地址：" + file);
      //  FileSystem fs = FileSystem.get(URI.create(file),config);
        Path path = new Path(file);
        //文件大小
        ContentSummary in = fs.getContentSummary(path);
        long size = in.getLength()/1024;
        LOG.info(String.format("%s文件大小为%sMB",path.getName(),size/1024));
        fs.close();
        return size;
    }

    /**
     * 读取文件大小
     * @param filePath
     * @throws IOException
     */
    public static long readDirectorySize(String filePath) throws IOException{
        Configuration config = new Configuration();

        String file = hdfsURL + filePath;
        FileSystem fs = FileSystem.get(URI.create(file),config);
        Path path = new Path(file);
        //文件大小
        ContentSummary in = fs.getContentSummary(path);
        long size = in.getSpaceConsumed();
        LOG.info(String.format("%s文件夹大小为(%s,%s,%s)",path.getName(), size, size/1024, size/(1024*1024)));
        fs.close();
        return size;
    }


    /**
     * 读取文件行数
     * @param filePath
     * 输入数据集地址均为HDFS绝对路径
     * @throws IOException
     */
    public static long readFileLine(String filePath) throws IOException{

        FileSystem fs = getFileSystem();
        Path path = new Path(filePath);
        //读取文件
        FSDataInputStream is=fs.open(path);
//        //读取文件
//        IOUtils.copyBytes(is, System.out, 2048, false); //复制到标准输出流
        //读取文件行数
        BufferedReader d = new BufferedReader(new InputStreamReader(is));
        long count = 0;
        String line;
        while ((line = d.readLine()) != null) {
            count += 1L;
        }
        LOG.info(String.format("%s文件行数为%s", path.getName(), count) );
        d.close();
        is.close();
        fs.close();
        return count;
    }

    /**
     * 读取文件并写入另一个文件
     * @param readFile、writeFile均为相对路径
     * @param
     * @throws IOException
     */
    public static void mergeFile(String readFile, String writeFile) throws IOException {

        String file = hdfsURL + readFile;
        FileSystem read = getFileSystem();
        Path readPath = new Path(file);
        //读取文件
        FSDataInputStream in = read.open(readPath);
        //
        file = hdfsURL + writeFile;
        FileSystem write = getFileSystem();
        Path writePath = new Path(file);

        //如果此文件不存在则创建新文件
        if (!write.exists(writePath)) {
            write.createNewFile(writePath);
        }
        FSDataOutputStream out = write.append(writePath);

        //读取文件并写入
        IOUtils.copyBytes(in, out, 4096, false); //复制到标准输出流


        out.write("\n".getBytes("UTF-8"));//换行
        write.close();
        read.close();
        in.close();
        out.close();
    }



    /**
     * 追加一行数据
     * @param line
     * @param filePath 相对路径
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void appendToHdfs(String line, String filePath) throws FileNotFoundException, IOException {
        String file = hdfsURL + filePath;
        FileSystem fs = getFileSystem();
        Path path = new Path(file);
        if(!fs.exists(path)){

        }
        //追加文件
        FSDataOutputStream out = fs.append(path);

        if(!line.equals("")) {
            out.write(line.getBytes());
            out.write("\n".getBytes());
        }

        out.close();
        fs.close();
    }
}

