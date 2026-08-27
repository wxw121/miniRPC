package com.minirpc.sample.api;

/**
 * 示例服务接口
 * <p>
 * 定义 RPC 服务的方法契约，由服务提供者实现，服务消费者通过代理调用。
 */
public interface HelloService {

    /**
     * 打招呼
     *
     * @param name 名字
     * @return 问候语
     */
    String sayHello(String name);

    /**
     * 加法运算
     *
     * @param a 加数
     * @param b 加数
     * @return 和
     */
    int add(int a, int b);
}
