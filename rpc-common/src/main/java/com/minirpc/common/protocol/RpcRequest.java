package com.minirpc.common.protocol;

import java.io.Serializable;

/**
 * RPC 请求对象
 * <p>
 * 封装一次远程方法调用所需的全部信息：
 * - requestId: 请求唯一标识，用于请求-响应匹配
 * - interfaceName: 调用的服务接口全限定名
 * - methodName: 调用的方法名
 * - parameterTypeNames: 方法参数类型名称数组（普通调用和泛化调用统一使用，服务端解析为本地类型）
 * - parameters: 方法参数值数组
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
     * 方法声明中的参数类型名称数组，使用 {@link Class#getName()} 对应的名称，
     * 例如 java.lang.String、int，以及表示 String[] 的 [Ljava.lang.String;。
     * <p>
     * 使用 String 而非直接传输 Class 的原因：
     * 1. 用简单的类型名称描述方法签名，减少对序列化器处理 Class 对象的依赖。
     * 2. 泛化调用可以直接提供类型名称，无需为描述参数类型而先加载对应的 Class。
     * 3. 服务端在查找方法前统一解析为本地类型：基本类型单独映射，引用类型和数组
     *    通过 {@code Class.forName()} 加载。
     * <p>
     * Hessian2 本身支持序列化 Class；传递名称并不能自动解决跨 ClassLoader 或类缺失问题。
     * 当前解析使用 RpcRequest 的定义类加载器，仍须能加载到与服务方法声明一致的类型。
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
        return switch (className) {
            case "int" -> int.class;
            case "long" -> long.class;
            case "double" -> double.class;
            case "float" -> float.class;
            case "boolean" -> boolean.class;
            case "byte" -> byte.class;
            case "short" -> short.class;
            case "char" -> char.class;
            case "void" -> void.class;
            default -> Class.forName(className);
        };
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
                ", parameterCount=" + (parameters == null ? 0 : parameters.length) +
                '}';
    }
}
