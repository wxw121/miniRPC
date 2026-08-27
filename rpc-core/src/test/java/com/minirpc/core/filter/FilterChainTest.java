package com.minirpc.core.filter;

import com.minirpc.common.protocol.RpcRequest;
import com.minirpc.common.protocol.RpcResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * FilterChain 责任链单元测试
 * <p>
 * 验证 Filter 的执行顺序、链式调用、空链处理
 */
class FilterChainTest {

    @Test
    @DisplayName("空 FilterChain 直接执行 finalInvoker")
    void testEmptyChain() {
        FilterChain chain = new FilterChain();
        RpcRequest request = new RpcRequest();
        request.setRequestId("test-001");

        RpcResponse response = chain.execute(request, req ->
                RpcResponse.success(req.getRequestId(), "direct"));

        assertEquals("direct", response.getData());
    }

    @Test
    @DisplayName("单个 Filter 正确拦截请求")
    void testSingleFilter() {
        FilterChain chain = new FilterChain();
        List<String> executionOrder = new ArrayList<>();

        chain.addFilter(new RpcFilter() {
            @Override
            public RpcResponse filter(RpcRequest request, FilterInvoker invoker) {
                executionOrder.add("before");
                RpcResponse resp = invoker.invoke(request);
                executionOrder.add("after");
                return resp;
            }

            @Override
            public int getOrder() {
                return 0;
            }
        });

        RpcRequest request = new RpcRequest();
        request.setRequestId("test-002");

        RpcResponse response = chain.execute(request, req -> {
            executionOrder.add("invoke");
            return RpcResponse.success(req.getRequestId(), "result");
        });

        assertEquals(List.of("before", "invoke", "after"), executionOrder);
        assertEquals("result", response.getData());
    }

    @Test
    @DisplayName("多个 Filter 按 order 排序执行")
    void testMultipleFiltersOrder() {
        FilterChain chain = new FilterChain();
        List<String> executionOrder = new ArrayList<>();

        // 先添加 order=10 的 Filter
        chain.addFilter(new RpcFilter() {
            @Override
            public RpcResponse filter(RpcRequest request, FilterInvoker invoker) {
                executionOrder.add("filter-B");
                return invoker.invoke(request);
            }

            @Override
            public int getOrder() {
                return 10;
            }
        });

        // 再添加 order=-100 的 Filter（应排在前面）
        chain.addFilter(new RpcFilter() {
            @Override
            public RpcResponse filter(RpcRequest request, FilterInvoker invoker) {
                executionOrder.add("filter-A");
                return invoker.invoke(request);
            }

            @Override
            public int getOrder() {
                return -100;
            }
        });

        RpcRequest request = new RpcRequest();
        request.setRequestId("test-003");

        chain.execute(request, req -> {
            executionOrder.add("final");
            return RpcResponse.success(req.getRequestId(), null);
        });

        // order=-100 的 filter-A 应先执行
        assertEquals(List.of("filter-A", "filter-B", "final"), executionOrder);
    }

    @Test
    @DisplayName("Filter 可以修改响应")
    void testFilterModifiesResponse() {
        FilterChain chain = new FilterChain();

        chain.addFilter(new RpcFilter() {
            @Override
            public RpcResponse filter(RpcRequest request, FilterInvoker invoker) {
                RpcResponse resp = invoker.invoke(request);
                // 修改响应数据
                return RpcResponse.success(request.getRequestId(), resp.getData() + "-modified");
            }

            @Override
            public int getOrder() {
                return 0;
            }
        });

        RpcRequest request = new RpcRequest();
        request.setRequestId("test-004");

        RpcResponse response = chain.execute(request, req ->
                RpcResponse.success(req.getRequestId(), "original"));

        assertEquals("original-modified", response.getData());
    }

    @Test
    @DisplayName("Filter 可以短路（不调用 invoker）")
    void testFilterShortCircuit() {
        FilterChain chain = new FilterChain();
        List<String> executionOrder = new ArrayList<>();

        chain.addFilter(new RpcFilter() {
            @Override
            public RpcResponse filter(RpcRequest request, FilterInvoker invoker) {
                executionOrder.add("blocker");
                // 不调用 invoker，直接返回（模拟限流/熔断）
                return RpcResponse.fail(request.getRequestId(), "blocked");
            }

            @Override
            public int getOrder() {
                return 0;
            }
        });

        RpcRequest request = new RpcRequest();
        request.setRequestId("test-005");

        RpcResponse response = chain.execute(request, req -> {
            executionOrder.add("should-not-reach");
            return RpcResponse.success(req.getRequestId(), "ok");
        });

        assertEquals(List.of("blocker"), executionOrder);
        assertEquals(RpcResponse.STATUS_FAIL, response.getStatus());
    }
}
