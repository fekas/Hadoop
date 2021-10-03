package com.zhongbin.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * 客户端代码常用套路
 * 1、获取客户端对象
 * 2、执行操作命令
 * 3、关闭资源
 * HDFS zookeeper
 */

public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:8020");

        //创建一个配置文件
        Configuration configuration = new Configuration();

        configuration.set("dfs.replication", "2");
        //用户
        String user = "dabin";

        fs = FileSystem.get(uri, configuration, user);

    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {

        fs.mkdirs(new Path("/xiyouji/huaguoshan"));
    }

    @Test
    public void testPut() throws IOException {
        //是否删除原数据，是否允许覆盖，原路径，目的路径
        fs.copyFromLocalFile(false, true, new Path("C:\\monkeyKing.txt"), new Path("/xiyouji/huaguoshan"));

    }

    @Test
    public void testGet() throws IOException {
        //是否删除原文件，原文件路径，目标地址路径，安全校验
        fs.copyToLocalFile(false, new Path("/xiyouji/huaguoshan"), new Path("C:\\"), false);
    }

    @Test
    public void testRm() throws IOException {
        //路径，是否递归删除
        fs.delete(new Path("/input"), false);
    }

    @Test
    public void testMv() throws IOException {
        //原文件路径，目标文件路径
        fs.rename(new Path("/input/word.txt"), new Path("/input/www.txt"));
    }

    @Test
    public void fileDetails() throws IOException {
        //获取文件信息
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        //遍历文件
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("=====" + fileStatus.getPath() + "======");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getModificationTime());

            //获取快信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();

            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void testFile() throws IOException {
        //判断文件夹还是文件
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus status : listStatus) {
            if (status.isFile()) {
                System.out.println("文件" + status.getPath().getName());
            }else{
                System.out.println("目录" + status.getPath().getName());
            }
        }
    }
}
