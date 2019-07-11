package net.noyark.demo;


import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestZookeeper {

    ZooKeeper zooKeeper;

    //-- zk服务器ip地址和端口
    //-- 客户端连接服务端的超市时间，单位毫秒
    //-- watcher接口，监听接口
    //-- 连接时，如果报 connect refuse异常，检查防火墙
    //-- 因为zookeeper底层用netty通信框架
    //-- 二netty底层用的nio通信
    //-- service iptables stop
    //--
    @Before
    public void test() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper("localhost", 30000,(e)->{
            System.out.println(e.getState());
            if (e.getState().equals(Watcher.Event.KeeperState.SyncConnected)){
                System.out.println("连接成功");
                countDownLatch.countDown();
            }
        });
        countDownLatch.await();
    }

    @Test

    public void create() throws Exception{
        //acl 节点权限
        String str = zooKeeper.create("/park02","hello".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(str);
    }

    @Test
    public void getData() throws Exception{
        byte[] bytes = zooKeeper.getData("/park01",null,null);
        System.out.println(new String(bytes));
    }

    public void setData() throws Exception{
        //-- 1参数：路径 2 参数：更新数据 3数据版本号
        //-- 数据版本号没更新一次，递增1
        //-- 一半写成-1，表示无论版本号是多少都会更新
        zooKeeper.setData("/park01","1234".getBytes(),-1);
    }

    @Test
    public void delete() throws Exception{

        zooKeeper.delete("/park01",-1);
    }

    /**
     * 子节点名称
     * @throws Exception
     */
    @Test
    public void getChild() throws Exception{
        List<String> paths = zooKeeper.getChildren("/park02",null);
        System.out.println(paths);//park03
    }

    @Test
    public void watch_data_changed() throws Exception{
        for(;;) {
            CountDownLatch latch = new CountDownLatch(1);
            String str = new String(zooKeeper.getData("/park02", (e) -> {
                if (e.getType().equals(Watcher.Event.EventType.NodeDataChanged))
                    System.out.println("数据发送变化");
                latch.countDown();
            }, null));
            System.out.println(str);
            latch.await();
        }
    }
    @Test
    public void watch_delete() throws Exception{
        zooKeeper.exists("/park01", new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if(watchedEvent.getType().equals(Event.EventType.NodeDeleted)){
                    System.out.println("节点被删除");
                }
            }
        });
        while (true);
    }

    /**
     * 监听指定节点下子节点创建或删除的事件
     * @throws Exception
     */
    @Test
    public void watch_child_changed() throws Exception{
        for(;;){

            CountDownLatch latch = new CountDownLatch(1);
            zooKeeper.getChildren("/park01", new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if(watchedEvent.getType().equals(Event.EventType.NodeDataChanged)){
                        System.out.println("有子节点被改变");
                        try{
                            List<String> paths = zooKeeper.getChildren("/park01",null);
                            for(String path:paths){
                                path = "/park01/"+path;
                                zooKeeper.getData(path, new Watcher() {
                                    @Override
                                    public void process(WatchedEvent watchedEvent) {
                                        if(watchedEvent.getType().equals(Event.EventType.NodeDataChanged)){
                                            System.out.println("数据发生变化");
                                        }
                                    }
                                },null);
                            }
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    latch.countDown();
                }
            });
            latch.await();
        }
    }

}
