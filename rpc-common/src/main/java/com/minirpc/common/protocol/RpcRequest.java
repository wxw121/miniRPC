package com.minirpc.common.protocol;

import java.io.Serializable;
import java.util.Arrays;

/**
 * RPC 请求对象
 * <p>
 * 封装一次远程方法调用所需的全部信息：
 * - requestId: 请求唯一标识，用于请求-响应匹配
 * - interfaceName: 调用的服务接口全限定名
 * - methodName: 调用的方法名
 * - parameterTypeNames: 方法参数类型名称数组（用 String 而非 Class，避免跨 ClassLoader 序列化问题）
 * - parameters: 方法参数值数组
 * <p>
 * 面试考点：
 * - 为什么用 String[] 而不是 Class<?>[]？
 *   Hessian2 序列化 Class 对象时，跨 ClassLoader 反序列化会失败。
 *   用类名字符串传输，服务端通过 Class.forName() 解析，更安全可靠。
 */
public class RpcRequest implements Serializable {

    private static final long serialVersionUID = 2L;

    /**
     * 请求唯一ID，用于请求-响应匹配
     */
    private String requestId;

    /**
     * 服务接口全限定名
     */
    private String interfaceName;

    /**
     * Service version requested by the consumer.
     */
    private String version = "1.0.0";

    /**
     * Service group requested by the consumer.
     */
    private String group = "default";

    /** Request lifecycle metadata, including tracing and the absolute deadline. */
    private RpcContext context;

    /**
     * 方法名
     */
    private String methodName;

    /**
     * 参数类型名称数组（全限定类名）
     * <p>
     * 使用 String 而非 Class 的原因：
     * 1. Class 对象在 Hessian2 序列化时可能失败（跨 ClassLoader）
     * 2. String 更轻量，网络传输开销更小
     * 3. 服务端通过 Class.forName() 解析，兼容性好
     */
    private String[] parameterTypeNames;

    /**
     * 参数值数组
     */
    private Object[] parameters;

    public RpcRequest() {
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getInterfaceName() {
        return interfaceName;
    }

    public void setInterfaceName(String interfaceName) {
        this.interfaceName = interfaceName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public RpcContext getContext() {
        return context;
    }

    public void setContext(RpcContext context) {
        this.context = context;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String[] getParameterTypeNames() {
        return parameterTypeNames;
    }

    public void setParameterTypeNames(String[] parameterTypeNames) {
        this.parameterTypeNames = parameterTypeNames;
    }

    /**
     * 便捷方法：从 Class<?>[] 设置参数类型名称
     */
    public void setParameterTypes(Class<?>[] parameterTypes) {
        if (parameterTypes == null) {
            this.parameterTypeNames = null;
            return;
        }
        this.parameterTypeNames = new String[parameterTypes.length];
        for (int i = 0; i < parameterTypes.length; i++) {
            this.parameterTypeNames[i] = parameterTypes[i].getName();
        }
    }

    /**
     * 解析参数类型为 Class<?>[]（服务端使用）
     * <p>
     * 支持基本类型（int, long, boolean 等）和引用类型
     */
    public Class<?>[] getParameterTypes() throws ClassNotFoundException {
        if (parameterTypeNames == null) {
            return new Class<?>[0];
        }
        Class<?>[] types = new Class<?>[parameterTypeNames.length];
        for (int i = 0; i < parameterTypeNames.length; i++) {
            types[i] = resolveClass(parameterTypeNames[i]);
        }
        return types;
    }

    /**
     * 解析类名（支持基本类型）
     */
    private static Class<?> resolveClass(String className) throws ClassNotFoundException {
        switch (className) {
            case "int": return int.class;
            case "long": return long.class;
            case "double": return double.class;
            case "float": return float.class;
            case "boolean": return boolean.class;
            case "byte": return byte.class;
            case "short": return short.class;
            case "char": return char.class;
            case "void": return void.class;
            default: return Class.forName(className);
        }
    }

    public Object[] getParameters() {
        return parameters;
    }

    public void setParameters(Object[] parameters) {
        this.parameters = parameters;
    }

    @Override
    public String toString() {
        return "RpcRequest{" +
                "requestId='" + requestId + '\'' +
                ", interfaceName='" + interfaceName + '\'' +
                ", methodName='" + methodName + '\'' +
                ", parameterTypeNames=" + Arrays.toString(parameterTypeNames) +
                ", parameters=" + Arrays.toString(parameters) +
                '}';
    }
}
