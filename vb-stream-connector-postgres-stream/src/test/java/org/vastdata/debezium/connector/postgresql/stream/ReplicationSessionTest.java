package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.vastdata.debezium.connector.postgresql.stream.protocol.StreamingMode;

import java.lang.reflect.Array;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 复制会话离线单测:引擎 {@code PgReplicationSessionTest}(140 行)的可离线翻译——
 * capFeedback 三态、drainPending 三形态(取尽/空轮/零载荷);外加同形补测四组:
 * Parameters 的 URL 形态、槽选项恰四项、ensureSlot 的 42710 复用语义(JDK 动态代理假
 * Connection,零第三方 mock)、run 循环五步序(假 PGReplicationStream 驱动到 isClosed
 * 守卫退出)。引擎侧需真库的会话行为(open 双连接/start 对真流/ensureSlot 真建槽/close
 * 次序端到端/确认位点被服务端采纳)由引擎 it 包覆盖({@code RawSessionContractTest}/
 * {@code FrontierCapTest}/{@code ReaderThroughputTest}),本模块对应面归 Task 8 的
 * Testcontainers IT——本类零网络零容器。
 */
class ReplicationSessionTest {

    /** 前沿 ≤0(尚未有任何事务输出,含负数防御值)不封顶:反馈值原样返回已收到的 LSN。 */
    @Test
    void zeroOrNegativeFrontierMeansNoCap() {
        assertEquals(500L, ReplicationSession.capFeedback(500L, 0L));
        assertEquals(500L, ReplicationSession.capFeedback(500L, -1L));
    }

    /** 正前沿取 min 封顶;前沿不会超过已收到(防御性取 min,超过时返回已收到值——不得确认未收到的位点)。 */
    @Test
    void positiveFrontierCapsToMinimum() {
        assertEquals(300L, ReplicationSession.capFeedback(500L, 300L));
        assertEquals(300L, ReplicationSession.capFeedback(300L, 500L));  // 前沿不会超过已收到,防御性取 min
    }

    /**
     * drainPending 一次调用取尽缓冲全部消息:500 条按序逐条回调、内容逐条等值、返回 true——
     * 调用方一轮即可搬空积压,不被"每条一拍"的节拍钉死(引擎 2026-08-31 吞吐冒烟踩坑的
     * 修复锚点:旧形态每轮一条 + 固定 sleep 把读取上限钉死 ~10 msg/s)。
     */
    @Test
    void drainPendingTakesAllBufferedMessagesInOneCall() throws Exception {
        FakeReplicationStream stream = new FakeReplicationStream();
        List<byte[]> received = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            stream.pending.add(ByteBuffer.wrap(("msg-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        boolean any = ReplicationSession.drainPending(stream, received::add);
        assertTrue(any, "有缓冲消息时必须返回 true");
        assertEquals(500, received.size(), "一次调用必须取尽全部缓冲消息");
        for (int i = 0; i < 500; i++) {
            assertEquals("msg-" + i, new String(received.get(i), StandardCharsets.UTF_8), "第 " + i + " 条内容/顺序不符");
        }
    }

    /** 空缓冲一轮:返回 false、零回调——调用方据此判定空转并进入 sleep 间歇。 */
    @Test
    void drainPendingReturnsFalseWhenNothingBuffered() throws Exception {
        FakeReplicationStream stream = new FakeReplicationStream();
        List<byte[]> received = new ArrayList<>();
        assertFalse(ReplicationSession.drainPending(stream, received::add), "空缓冲必须返回 false");
        assertTrue(received.isEmpty(), "空缓冲不得回调");
    }

    /** remaining()==0 的零载荷防御跳过:不回调但同样被消费掉,drain 继续取后续真实消息并正常终止。 */
    @Test
    void drainPendingSkipsZeroLengthPayload() throws Exception {
        FakeReplicationStream stream = new FakeReplicationStream();
        stream.pending.add(ByteBuffer.wrap(new byte[0]));
        stream.pending.add(ByteBuffer.wrap(new byte[] {1, 2}));
        List<byte[]> received = new ArrayList<>();
        assertTrue(ReplicationSession.drainPending(stream, received::add));
        assertEquals(1, received.size(), "零载荷必须被跳过、真实消息必须到达");
        assertTrue(Arrays.equals(new byte[] {1, 2}, received.get(0)), "真实消息内容应逐字节等值");
    }

    /**
     * 复制 URL 形态:jdbcUrl 为三段式普通地址;replicationUrl 必须同时追加
     * replication=database 与 assumeMinServerVersion=9.4——后者缺失时 pgjdbc 不把
     * replication 参数放进启动包,START_REPLICATION 被服务端按普通会话解析报语法错
     * (引擎真实 PG 18 集成首跑发现)。
     */
    @Test
    void replicationUrlAppendsReplicationAndMinServerVersionParams() {
        ReplicationSession.Parameters params = parameters(StreamingMode.ON, true, 10);
        assertEquals("jdbc:postgresql://localhost:55432/postgres", params.jdbcUrl(),
                "普通 JDBC URL 应为 host:port/database 三段式");
        assertEquals(params.jdbcUrl() + "?replication=database&assumeMinServerVersion=9.4", params.replicationUrl(),
                "复制 URL 必须同时携带 replication=database 与 assumeMinServerVersion=9.4");
    }

    /**
     * 槽选项恰四项:proto_version/publication_names/streaming/two_phase,值随 Parameters
     * 映射(档位与开关逐项对应),插入序固定可复现——多出第五项或漏项都会改变服务端解码
     * 行为(proto_version/streaming/two_phase 是流式路径的硬前置)。
     */
    @Test
    void slotOptionsAreExactlyTheFourProtocolOptions() {
        Map<String, String> on = ReplicationSession.slotOptions(parameters(StreamingMode.ON, true, 10));
        assertEquals(4, on.size(), "槽选项必须恰四项,不得多不得少");
        assertEquals("4", on.get("proto_version"), "proto_version 应取参数的协议版本");
        assertEquals("vb_pub", on.get("publication_names"), "publication_names 应取参数的 publication 名");
        assertEquals("on", on.get("streaming"), "streaming 应映射档位参数");
        assertEquals("on", on.get("two_phase"), "two_phase=true 应映射 on");
        assertEquals(List.of("proto_version", "publication_names", "streaming", "two_phase"), new ArrayList<>(on.keySet()),
                "选项插入序固定,拼装可测可复现");

        Map<String, String> off = ReplicationSession.slotOptions(parameters(StreamingMode.OFF, false, 10));
        assertEquals(4, off.size(), "OFF 档位同样恰四项");
        assertEquals("off", off.get("streaming"), "OFF 档位 streaming=off");
        assertEquals("off", off.get("two_phase"), "two_phase=false 应映射 off");
    }

    /**
     * ensureSlot 的 42710 复用语义:SQLState 42710(duplicate_object)吞掉复用既有槽,
     * 其余 SQLState 原样上抛;建槽 SQL 文本与两个绑定参数固定(第 4 参 twoPhase 随槽声明)。
     * 引擎原用例经真库覆盖(建槽/复用副作用),归 Task 8 IT;本用例以动态代理假 Connection
     * 锚定分支语义与 SQL 契约。
     */
    @Test
    void ensureSlotSwallowsDuplicateObjectButRethrowsOtherSqlStates() {
        List<String> capturedSql = new ArrayList<>();
        List<Object> capturedParams = new ArrayList<>();

        assertDoesNotThrow(() -> ReplicationSession.ensureSlot(
                fakeConnection("42710", capturedSql, capturedParams), parameters(StreamingMode.ON, true, 10)),
                "duplicate_object 表示槽已存在,应 WARN 复用而非失败");
        assertEquals(List.of("SELECT pg_create_logical_replication_slot(?, 'pgoutput', false, ?)"), capturedSql,
                "建槽 SQL 文本固定(第 3 参临时槽=false、第 4 参 two_phase 绑定)");
        assertEquals(Arrays.asList("vb_cdc_slot", Boolean.TRUE), capturedParams,
                "绑定参数应为槽名与 twoPhase");

        List<String> okSql = new ArrayList<>();
        assertDoesNotThrow(() -> ReplicationSession.ensureSlot(
                fakeConnection(null, okSql, new ArrayList<>()), parameters(StreamingMode.ON, true, 10)),
                "executeQuery 正常返回时应走成功路径(只需副作用)");

        SQLException thrown = assertThrows(SQLException.class, () -> ReplicationSession.ensureSlot(
                fakeConnection("42P01", new ArrayList<>(), new ArrayList<>()), parameters(StreamingMode.ON, true, 10)),
                "非 42710 的 SQLException 必须原样上抛");
        assertEquals("42P01", thrown.getSQLState(), "上抛的异常不得被吞改");
    }

    /**
     * run 循环五步序(假流全离线驱动;引擎侧该序由 IT 间接覆盖,connector 侧归 Task 8):
     * ①isClosed 守卫——流被置 closed 后下一轮以 SQLException 退出;②drain 取尽并按序回调
     * (receivedAny 轮立即续转);③每轮 capFeedback 封顶后同值写 applied 与 flushed(收到
     * 500、前沿 300 → 确认 300,不越过前沿);④未满 feedbackIntervalSeconds 不
     * forceUpdateStatus(10s 间隔在本用例 ~200ms 内零次);⑤空轮 sleep 100ms——三次空轮
     * 探测后退出,两轮休眠保证总耗时下界 200ms。
     */
    @Test
    void runLoopFollowsFiveStepOrderUntilClosedGuardExits() {
        RunLoopStream stream = new RunLoopStream(2, 3);
        stream.lastReceive = 500L;
        List<String> received = new ArrayList<>();

        long startedNanos = System.nanoTime();
        SQLException exited = assertThrows(SQLException.class,
                () -> ReplicationSession.run(stream, parameters(StreamingMode.ON, true, 10),
                        raw -> received.add(new String(raw, StandardCharsets.UTF_8)), () -> 300L));
        long elapsedMillis = (System.nanoTime() - startedNanos) / 1_000_000L;

        assertEquals("复制流已结束（连接断开）", exited.getMessage(), "①closed 后应经 isClosed 守卫以 SQLException 退出");
        assertEquals(List.of("m-0", "m-1"), received, "②drain 应按序交付全部缓冲消息");
        assertEquals(List.of(300L, 300L, 300L), stream.applied,
                "③每轮封顶值应写入 applied(收到 500、前沿 300 → 300,共三轮反馈)");
        assertEquals(List.of(300L, 300L, 300L), stream.flushed, "③applied 与 flushed 应同值成对写入");
        assertEquals(0, stream.forceUpdates, "④未满反馈间隔不得 forceUpdateStatus");
        assertTrue(elapsedMillis >= 200, "⑤两次空轮各睡 100ms,总耗时下界 200ms,实测 " + elapsedMillis);
    }

    /**
     * 组装测试参数包:取引擎 src/docker 冒烟环境的同形坐标(localhost:55432/postgres,
     * 槽 vb_cdc_slot、publication vb_pub、proto 4),档位/开关/反馈间隔由用例注入。
     *
     * @param mode 流式档位
     * @param twoPhase 两阶段开关
     * @param feedbackIntervalSeconds 反馈间隔(秒)
     * @return 可直接驱动静态纯函数/静态接缝的参数包
     */
    private static ReplicationSession.Parameters parameters(StreamingMode mode, boolean twoPhase, int feedbackIntervalSeconds) {
        return new ReplicationSession.Parameters("localhost", 55432, "postgres", "postgres", "postgres",
                "vb_cdc_slot", "vb_pub", 4, mode, twoPhase, feedbackIntervalSeconds);
    }

    /**
     * 构造只应答 ensureSlot 路径的假 Connection(JDK 动态代理,零第三方 mock 依赖):
     * prepareStatement(sql) 记下 SQL 并返回记录型假 PreparedStatement——setString/setBoolean
     * 按序记参,executeQuery 在 failSqlState 非 null 时抛该 SQLState 的 SQLException
     * (null 则返回空假 ResultSet 走成功路径);其余接口方法经 fallback 回落,不参与断言。
     *
     * @param failSqlState executeQuery 应抛出的 SQLState;null 表示走成功路径
     * @param capturedSql 出参:捕获的 prepareStatement SQL 文本(每调用一条)
     * @param capturedParams 出参:按序捕获的 setString/setBoolean 绑定参数
     * @return 可直接交给 ReplicationSession.ensureSlot 静态接缝的假 Connection
     */
    private static Connection fakeConnection(String failSqlState, List<String> capturedSql, List<Object> capturedParams) {
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                ReplicationSessionTest.class.getClassLoader(),
                new Class<?>[]{ PreparedStatement.class },
                (proxy, method, args) -> {
                    String name = method.getName();
                    if (name.equals("setString") || name.equals("setBoolean")) {
                        capturedParams.add(args[1]);
                        return null;
                    }
                    if (name.equals("executeQuery")) {
                        if (failSqlState != null) {
                            throw new SQLException("fake executeQuery failure", failSqlState);
                        }
                        return (ResultSet) Proxy.newProxyInstance(
                                ReplicationSessionTest.class.getClassLoader(),
                                new Class<?>[]{ ResultSet.class },
                                (rsProxy, rsMethod, rsArgs) ->
                                        rsMethod.getName().equals("next") ? Boolean.FALSE : fallback(rsMethod.getReturnType()));
                    }
                    return fallback(method.getReturnType());
                });
        return (Connection) Proxy.newProxyInstance(
                ReplicationSessionTest.class.getClassLoader(),
                new Class<?>[]{ Connection.class },
                (proxy, method, args) -> {
                    if (method.getName().equals("prepareStatement")) {
                        capturedSql.add((String) args[0]);
                        return statement;
                    }
                    return fallback(method.getReturnType());
                });
    }

    /**
     * 质朴的接口方法回落值:非 void 基本类型给零值(经单元素原生数组反射取默认值)、
     * 对象与 void 类型给 null(void 返回值被代理层忽略)——让假件对未关心的接口方法
     * 静默通过而不抛 UnsupportedOperation。
     *
     * @param returnType 接口方法的返回类型
     * @return 该类型的默认值
     */
    private static Object fallback(Class<?> returnType) {
        if (returnType != void.class && returnType.isPrimitive()) {
            return Array.get(Array.newInstance(returnType, 1), 0);
        }
        return null;
    }

    /**
     * 测试用假复制流(drainPending 三用例):预置消息队列供 readPending 按序消费、取尽返回
     * null(非阻塞语义),其余接口方法与 drainPending 无关,空实现/抛异常即可。仅供本测试
     * 类直连,不模拟网络与协议。
     */
    private static final class FakeReplicationStream implements PGReplicationStream {

        private final Queue<ByteBuffer> pending = new ArrayDeque<>();

        @Override
        public ByteBuffer readPending() {
            return pending.poll();
        }

        @Override
        public ByteBuffer read() {
            throw new UnsupportedOperationException("drainPending 只走 readPending,阻塞读不参与测试");
        }

        @Override
        public LogSequenceNumber getLastReceiveLSN() {
            return LogSequenceNumber.INVALID_LSN;
        }

        @Override
        public LogSequenceNumber getLastFlushedLSN() {
            return LogSequenceNumber.INVALID_LSN;
        }

        @Override
        public LogSequenceNumber getLastAppliedLSN() {
            return LogSequenceNumber.INVALID_LSN;
        }

        @Override
        public void setFlushedLSN(LogSequenceNumber flushed) {
            // drainPending 不触碰反馈状态,空实现
        }

        @Override
        public void setAppliedLSN(LogSequenceNumber applied) {
            // 同上
        }

        @Override
        public void forceUpdateStatus() {
            // 同上
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {
            // 测试不关流
        }
    }

    /**
     * run 循环专用假流:构造时预置 messageCount 条消息;readPending 每次空手而归计数一次
     * 空轮,累计 emptyPollsBeforeClose 次后置 closed(驱动 isClosed 守卫退出);记录
     * setAppliedLSN/setFlushedLSN 的每次值与 forceUpdateStatus 次数供③④断言。
     * lastReceive 可注入 getLastReceiveLSN 的返回值。仅供本测试类直连,单线程(调用方
     * 线程同步驱动 run)读写无并发。
     */
    private static final class RunLoopStream implements PGReplicationStream {

        private final Queue<ByteBuffer> pending = new ArrayDeque<>();
        private final List<Long> applied = new ArrayList<>();
        private final List<Long> flushed = new ArrayList<>();
        private int emptyPollsBeforeClose;
        private int forceUpdates;
        private boolean closed;
        private long lastReceive;

        RunLoopStream(int messageCount, int emptyPollsBeforeClose) {
            for (int i = 0; i < messageCount; i++) {
                pending.add(ByteBuffer.wrap(("m-" + i).getBytes(StandardCharsets.UTF_8)));
            }
            this.emptyPollsBeforeClose = emptyPollsBeforeClose;
        }

        @Override
        public ByteBuffer readPending() {
            ByteBuffer buffer = pending.poll();
            if (buffer == null && --emptyPollsBeforeClose <= 0) {
                closed = true;
            }
            return buffer;
        }

        @Override
        public ByteBuffer read() {
            throw new UnsupportedOperationException("run 循环只走 readPending,阻塞读不参与测试");
        }

        @Override
        public LogSequenceNumber getLastReceiveLSN() {
            return LogSequenceNumber.valueOf(lastReceive);
        }

        @Override
        public LogSequenceNumber getLastFlushedLSN() {
            return LogSequenceNumber.INVALID_LSN;
        }

        @Override
        public LogSequenceNumber getLastAppliedLSN() {
            return LogSequenceNumber.INVALID_LSN;
        }

        @Override
        public void setFlushedLSN(LogSequenceNumber lsn) {
            flushed.add(lsn.asLong());
        }

        @Override
        public void setAppliedLSN(LogSequenceNumber lsn) {
            applied.add(lsn.asLong());
        }

        @Override
        public void forceUpdateStatus() {
            forceUpdates++;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            // 测试不关流
        }
    }
}
