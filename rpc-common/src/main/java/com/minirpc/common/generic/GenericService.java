package com.minirpc.common.generic;

/**
 * 泛化调用接口
 * 调用方无需依赖服务接口 JAR，通过方法名 + 参数类型 + 参数值直接调用
 * 
 * 使用场景：
 * - 网关/测试平台：不依赖具体接口 JAR，动态调用任意服务
 * - 跨语言调用：前端传入 JSON 描述，后端泛化调用
 * 
 * 使用示例：
 * <pre>
 * GenericService genericService = rpcClient.createGenericProxy("com.example.EchoService");
 * Object result = genericService.$invoke("echo", new String[]{"java.lang.String"}, new Object[]{"hello"});
 * </pre>
 */
public interface GenericService {

    /**
     * 泛化调用
     *
     * @param methodName     方法名
     * @param parameterTypes 参数类型全限定名数组
     * @param args           参数值数组
     * @return 调用结果
     */
    Object $invoke(String methodName, String[] parameterTypes, Object[] args);
}
