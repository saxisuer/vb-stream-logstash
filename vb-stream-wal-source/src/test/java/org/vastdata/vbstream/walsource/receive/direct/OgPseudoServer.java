package org.vastdata.vbstream.walsource.receive.direct;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * openGauss 物理复制伪服务器（裸 v3 协议测试基建）：本地 {@link ServerSocket} 手写 v3
 * 帧应答，覆盖 startup/认证/IDENTIFY_SYSTEM/START_REPLICATION/CopyBoth 流面的离线验收
 * （Task 5 五循环）。帧格式按 FE/BE 协议 v3（帧 = 类型 char + int32 大端长度[含自身] +
 * 载荷）；CopyData 内嵌复制消息（'w'/'k'/'r'）为 C 结构体 memcpy——小端、含尾部对齐
 * 垫（布局与 {@code walprotocol.h} 转录一致，字段出处见
 * {@link V3ProtocolChannel} 类注）。
 *
 * <p>线程模型：{@link #script(Script)} 在独立线程（og-pseudo-server）跑服务端脚本，
 * 客户端在被测线程运行；{@link #awaitScript(long)} join 并重抛脚本异常——脚本断言失败
 * 即测试失败。多连接场景（断线重连）逐次 script 即可（串行单连接，伪服务器同一时刻
 * 只服务最新 accept）。</p>
 */
final class OgPseudoServer implements AutoCloseable {

    /** 服务端脚本函数（抛 IOException 的 lambda 由本类包装收集）。 */
    @FunctionalInterface
    interface Script {
        /** 服务端应答脚本体——对单个连接的完整读写序列。 */
        void run(Session session) throws IOException;
    }

    private final ServerSocket serverSocket;

    /** 当前连接会话——accept 后发布，脚本线程独占写。 */
    private volatile Session session;

    /** 脚本线程异常收集——awaitScript 重抛给测试线程。 */
    private final AtomicReference<Throwable> scriptFailure = new AtomicReference<>();

    /** 已注册的脚本线程（多连接场景串行注册，awaitScript 逐段 join）。 */
    private final List<Thread> scriptThreads = new ArrayList<>();

    private OgPseudoServer(ServerSocket serverSocket) {
        this.serverSocket = serverSocket;
    }

    /**
     * 起伪服务器（回环地址、临时端口、backlog 1——离线测试串行单连接足够）。
     *
     * @return 就绪的伪服务器
     * @throws IOException ServerSocket 打开失败
     */
    static OgPseudoServer start() throws IOException {
        ServerSocket ss = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        return new OgPseudoServer(ss);
    }

    /**
     * 伪服务器监听端口。
     *
     * @return 绑定的临时端口号
     */
    int port() {
        return serverSocket.getLocalPort();
    }

    /**
     * 在独立线程跑一段服务端脚本：阻塞 accept 一个连接后执行脚本体。
     *
     * <p>边界：脚本抛出的任何 Throwable 都被收集（线程不静默死），由
     * {@link #awaitScript(long)} 重抛；同一时刻只支持一段在跑（脚本线程字段单槽）。</p>
     *
     * @param body 服务端应答脚本
     */
    void script(Script body) {
        Thread t = new Thread(() -> {
            try (Socket socket = serverSocket.accept()) {
                socket.setTcpNoDelay(true);
                Session s = new Session(socket);
                session = s;
                body.run(s);
            } catch (Throwable t1) {
                scriptFailure.compareAndSet(null, t1);
            }
        }, "og-pseudo-server");
        t.setDaemon(true);
        synchronized (scriptThreads) {
            scriptThreads.add(t);
        }
        t.start();
    }

    /**
     * 等待全部脚本线程结束并重抛其失败（脚本断言失败 = 测试失败）。
     *
     * @param timeoutMs join 上限（毫秒，逐线程计）
     * @throws Exception 脚本线程内抛出的原样异常；join 超时抛 AssertionError
     */
    void awaitScript(long timeoutMs) throws Exception {
        List<Thread> threads;
        synchronized (scriptThreads) {
            threads = new ArrayList<>(scriptThreads);
        }
        for (Thread t : threads) {
            t.join(timeoutMs);
            if (t.isAlive()) {
                throw new AssertionError("伪服务器脚本 " + timeoutMs + "ms 内未结束（客户端未走完预期序列？）");
            }
        }
        Throwable t = scriptFailure.get();
        if (t instanceof Exception e) {
            throw e;
        }
        if (t != null) {
            throw new AssertionError(t);
        }
    }

    /** 关伪服务器与在途会话（幂等清理路径）。 */
    @Override
    public void close() {
        Session s = session;
        if (s != null) {
            s.closeClientQuietly();
        }
        try {
            serverSocket.close();
        } catch (IOException e) {
            // 清理路径不抛
        }
    }

    /**
     * 单个客户端连接的服务端读写面：读 startup/'p'/'Q'/'d' 客户端帧，写 v3 应答帧。
     * 帧读写直接对 socket 流操作（无内部缓冲残留——逐帧全读全写）。
     */
    static final class Session {

        private final Socket socket;
        private final DataInputStream in;
        private final OutputStream out;

        Session(Socket socket) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new BufferedOutputStream(socket.getOutputStream());
        }

        // ---- 读面（客户端 → 服务端） ----

        /**
         * 读 startup 消息（无类型前缀：int32 总长 + int16 协议大版本 + int16 小版本 +
         * k\0v\0 参数对 + 终止 \0），返回参数表。
         *
         * <p>断言协议 3.0（裸客户端形态——openGauss libpq 同形）。</p>
         *
         * @return startup 参数表（保留插入序）
         * @throws IOException 流读失败
         */
        Map<String, String> readStartup() throws IOException {
            int len = in.readInt();
            byte[] body = new byte[len - 4];
            in.readFully(body);
            int major = ((body[0] & 0xFF) << 8) | (body[1] & 0xFF);
            int minor = ((body[2] & 0xFF) << 8) | (body[3] & 0xFF);
            if (major != 3 || minor != 0) {
                throw new AssertionError("startup 协议版本非 3.0: " + major + "." + minor);
            }
            Map<String, String> params = new LinkedHashMap<>();
            int i = 4;
            while (i < body.length && body[i] != 0) {
                int kEnd = i;
                while (body[kEnd] != 0) {
                    kEnd++;
                }
                String key = new String(body, i, kEnd - i, StandardCharsets.UTF_8);
                int vEnd = kEnd + 1;
                while (vEnd < body.length && body[vEnd] != 0) {
                    vEnd++;
                }
                params.put(key, new String(body, kEnd + 1, vEnd - kEnd - 1, StandardCharsets.UTF_8));
                i = vEnd + 1;
            }
            return params;
        }

        /**
         * 读下一帧并返回 (类型 char + 载荷) 的拼接字节（帧长前缀剥除；载荷全量）。
         *
         * @return 帧内容——首字节为消息类型字符，其后为载荷
         * @throws IOException 流读失败/对端关闭（EOF 上抛 EOFException）
         */
        byte[] readFrame() throws IOException {
            int type = in.readUnsignedByte();
            int len = in.readInt();
            byte[] payload = new byte[len - 4];
            in.readFully(payload);
            byte[] frame = new byte[1 + payload.length];
            frame[0] = (byte) type;
            System.arraycopy(payload, 0, frame, 1, payload.length);
            return frame;
        }

        /**
         * 读客户端 PasswordMessage（'p'）：返回密码摘要字符串（载荷剥除尾部 NUL）。
         *
         * @return 摘要（如 {@code md5…} 或 RFC5802 proof 十六进制串）
         * @throws IOException 帧类型非 'p' 时抛 AssertionError
         */
        String readPasswordDigest() throws IOException {
            byte[] frame = readFrame();
            if (frame[0] != 'p') {
                throw new AssertionError("期望 PasswordMessage 'p'，实收 '" + (char) frame[0] + "'");
            }
            return new String(frame, 1, frame.length - 2, StandardCharsets.UTF_8);
        }

        /**
         * 读客户端简单查询（'Q'）：返回 SQL 文本（剥 NUL）。
         *
         * @return SQL 语句
         * @throws IOException 帧类型非 'Q' 时抛 AssertionError
         */
        String readQuery() throws IOException {
            byte[] frame = readFrame();
            if (frame[0] != 'Q') {
                throw new AssertionError("期望 Query 'Q'，实收 '" + (char) frame[0] + "'");
            }
            return new String(frame, 1, frame.length - 2, StandardCharsets.UTF_8);
        }

        // ---- 写面（服务端 → 客户端） ----

        /**
         * 写通用 v3 帧（类型字符 + int32 大端长度[含自身] + 载荷）并刷出。
         *
         * @param type    消息类型字符
         * @param payload 载荷（无类型无长度）
         * @throws IOException 写失败
         */
        void writeMessage(char type, byte[] payload) throws IOException {
            out.write(type);
            out.write(int32(payload.length + 4));
            out.write(payload);
            out.flush();
        }

        /** 写 AuthenticationMD5Password 挑战（'R' 码 5 + 4 字节盐）。 */
        void writeAuthMd5(byte[] salt) throws IOException {
            byte[] code = int32(5);
            byte[] payload = new byte[8];
            System.arraycopy(code, 0, payload, 0, 4);
            System.arraycopy(salt, 0, payload, 4, 4);
            writeMessage('R', payload);
        }

        /**
         * 写 openGauss sha256 挑战（'R' 码 10 + 子码 0 + 64 字符盐 + 8 字符 token
         * [+ int32 迭代数——3.51 起追加形态，wire 布局见 V3ProtocolChannel#authenticate]）。
         */
        void writeAuthSha256(String saltHex64, String tokenHex8, Integer iterations) throws IOException {
            byte[] payload = new byte[8 + 64 + 8 + (iterations != null ? 4 : 0)];
            System.arraycopy(int32(10), 0, payload, 0, 4);
            System.arraycopy(int32(0), 0, payload, 4, 4);
            byte[] salt = saltHex64.getBytes(StandardCharsets.UTF_8);
            byte[] token = tokenHex8.getBytes(StandardCharsets.UTF_8);
            System.arraycopy(salt, 0, payload, 8, 64);
            System.arraycopy(token, 0, payload, 8 + 64, 8);
            if (iterations != null) {
                System.arraycopy(int32(iterations), 0, payload, 8 + 64 + 8, 4);
            }
            writeMessage('R', payload);
        }

        /** 写 AuthenticationOk（'R' 码 0）。 */
        void writeAuthOk() throws IOException {
            writeMessage('R', int32(0));
        }

        /**
         * 写 openGauss sha256 挑战的 3.0 形态（'R' 码 10 + 子码 0 + 64 字符盐 + 8 字符
         * token + 64 字符 sever_signature——auth.cpp 对 minor<GAUSS_BASE 客户端的形态，
         * 迭代数不携带）。签名可注入（坏签名面：客户端校验须拒绝）。
         */
        void writeAuthSha256V30(String saltHex64, String tokenHex8, String signatureHex64) throws IOException {
            byte[] payload = new byte[8 + 64 + 8 + 64];
            System.arraycopy(int32(10), 0, payload, 0, 4);
            System.arraycopy(int32(0), 0, payload, 4, 4);
            System.arraycopy(saltHex64.getBytes(StandardCharsets.UTF_8), 0, payload, 8, 64);
            System.arraycopy(tokenHex8.getBytes(StandardCharsets.UTF_8), 0, payload, 8 + 64, 8);
            System.arraycopy(signatureHex64.getBytes(StandardCharsets.UTF_8), 0, payload, 8 + 64 + 8, 64);
            writeMessage('R', payload);
        }

        /** 写 ParameterStatus（'S'）。 */
        void writeParameterStatus(String key, String value) throws IOException {
            byte[] k = key.getBytes(StandardCharsets.UTF_8);
            byte[] v = value.getBytes(StandardCharsets.UTF_8);
            byte[] payload = new byte[k.length + v.length + 2];
            System.arraycopy(k, 0, payload, 0, k.length);
            payload[k.length] = 0;
            System.arraycopy(v, 0, payload, k.length + 1, v.length);
            payload[payload.length - 1] = 0;
            writeMessage('S', payload);
        }

        /** 写 BackendKeyData（'K'，载荷任意——本客户端丢弃）。 */
        void writeBackendKeyData() throws IOException {
            writeMessage('K', new byte[8]);
        }

        /** 写 ReadyForQuery（'Z'，事务态空闲）。 */
        void writeReadyForQuery() throws IOException {
            writeMessage('Z', new byte[]{'I'});
        }

        /** 写 ErrorResponse（'E'）——字段 S/V/C/M + 终止 \0。 */
        void writeError(String sqlState, String message) throws IOException {
            byte[] s = sqlState.getBytes(StandardCharsets.UTF_8);
            byte[] m = message.getBytes(StandardCharsets.UTF_8);
            byte[] payload = new byte[2 + s.length + 2 + m.length + 2 + 1];
            int i = 0;
            payload[i++] = 'C';
            System.arraycopy(s, 0, payload, i, s.length);
            i += s.length;
            payload[i++] = 0;
            payload[i++] = 'M';
            System.arraycopy(m, 0, payload, i, m.length);
            i += m.length;
            payload[i++] = 0;
            payload[i++] = 0;
            writeMessage('E', payload);
        }

        /** 写 RowDescription（'T'）——n 列占位（本客户端只按列数走读，列定义不解析）。 */
        void writeRowDescription(int columns) throws IOException {
            byte[] payload = new byte[2 + columns * 19];
            payload[0] = 0;
            payload[1] = (byte) columns;
            writeMessage('T', payload);
        }

        /**
         * 写 DataRow（'D'）：int16 列数 + 逐列 int32 长度前缀文本值（与 IDENTIFY_SYSTEM
         * 应答同形——systemid/timeline/xlogpos/…）。
         */
        void writeDataRow(String... fields) throws IOException {
            int total = 2;
            byte[][] enc = new byte[fields.length][];
            for (int i = 0; i < fields.length; i++) {
                enc[i] = fields[i].getBytes(StandardCharsets.UTF_8);
                total += 4 + enc[i].length;
            }
            byte[] payload = new byte[total];
            int i = 0;
            payload[i++] = (byte) (fields.length >> 8);
            payload[i++] = (byte) fields.length;
            for (byte[] f : enc) {
                payload[i++] = (byte) (f.length >> 24);
                payload[i++] = (byte) (f.length >> 16);
                payload[i++] = (byte) (f.length >> 8);
                payload[i++] = (byte) f.length;
                System.arraycopy(f, 0, payload, i, f.length);
                i += f.length;
            }
            writeMessage('D', payload);
        }

        /** 写 CommandComplete（'C' + tag + NUL）。 */
        void writeCommandComplete(String tag) throws IOException {
            writeMessage('C', (tag + "\0").getBytes(StandardCharsets.UTF_8));
        }

        /** 写 CopyBothResponse（'W'）：整体拷贝模式 + int16 列数 0（物理流无列）。 */
        void writeCopyBothResponse() throws IOException {
            writeMessage('W', new byte[]{0, 0, 0});
        }

        /** 写 CopyData（'d'）包裹的复制消息载荷（首字节 'w'/'k'/'r'/'m'）。 */
        void writeCopyData(byte[] replicationPayload) throws IOException {
            writeMessage('d', replicationPayload);
        }

        /**
         * 写 'w' WAL 数据帧：'w' + WalDataMessageHeader（64B 小端 memcpy——dataStart/
         * walEnd/sendTime/四个 sender 位 + catchup + 对齐垫）+ WAL 载荷。
         */
        void writeWalData(long dataStart, long walEnd, byte[] walPayload) throws IOException {
            byte[] frame = new byte[65 + walPayload.length];
            frame[0] = 'w';
            putU64(frame, 1, dataStart);
            putU64(frame, 9, walEnd);
            putU64(frame, 17, 4242424242000L);          // sendTime：任意观测值
            putU64(frame, 25, walEnd);
            putU64(frame, 33, walEnd);
            putU64(frame, 41, walEnd);
            putU64(frame, 49, walEnd);
            frame[57] = 1;                              // catchup
            System.arraycopy(walPayload, 0, frame, 65, walPayload.length);
            writeCopyData(frame);
        }

        /**
         * 写 'k' keepalive 帧：'k' + WalSndrMessage（32B 小端 memcpy——walEnd/
         * peer_role/peer_state/sendTime/replyRequested/catchup + 对齐垫），总 33 字节
         * （STREAMING_KEEPALIVE_SIZE，receivelog.cpp:47）。
         */
        void writeKeepalive(long walEnd, boolean replyRequested) throws IOException {
            byte[] frame = new byte[33];
            frame[0] = 'k';
            putU64(frame, 1, walEnd);
            putI32(frame, 9, 2);                        // peer_role：任意观测值
            putI32(frame, 13, 1);                       // peer_state
            putU64(frame, 17, 4242424242000L);          // sendTime
            frame[25] = (byte) (replyRequested ? 1 : 0);
            frame[26] = 0;                              // catchup
            writeCopyData(frame);
        }

        /**
         * 读客户端 CopyData 反馈（'d' 包裹的 65B 'r'）并解析为字段视图——keepalive 即时
         * 回应/心跳节奏断言的读面。布局锚见 {@link V3ProtocolChannel#encodeStandbyReply}。
         *
         * @return StandbyReply 视图（sendTime/replyRequested 等字段可读）
         * @throws IOException 帧非 'd'+'r' 时抛 AssertionError
         */
        StandbyReply readStandbyReply() throws IOException {
            byte[] frame = readFrame();
            if (frame[0] != 'd' || frame[1] != 'r') {
                throw new AssertionError("期望 CopyData 'r' 反馈，实收 '" + (char) frame[0] + (char) frame[1] + "'");
            }
            return StandbyReply.decode(frame, 1);
        }

        /**
         * 带超时地收集在窗内到达的 'r' 反馈（心跳节奏量测）：soTimeout 轮询读，窗到即返。
         *
         * @param windowMs   量测窗（毫秒）
         * @param pollMs     单次读超时（毫秒）
         * @return 窗内到达的反馈列表（可能为空——由断言判节奏）
         * @throws IOException 非超时类读失败
         */
        List<StandbyReply> collectStandbyReplies(long windowMs, int pollMs) throws IOException {
            socket.setSoTimeout(pollMs);
            long deadline = System.currentTimeMillis() + windowMs;
            List<StandbyReply> replies = new ArrayList<>();
            while (System.currentTimeMillis() < deadline) {
                try {
                    replies.add(readStandbyReply());
                } catch (SocketTimeoutException e) {
                    // 窗内无反馈——继续等到窗尽
                }
            }
            socket.setSoTimeout(0);
            return replies;
        }

        /** 写 'm' 配置文件帧（服务端 GaussDB 配置推送——接收端须整体跳过，receivelog.cpp 'm' 分支）。 */
        void writeConfigFrame() throws IOException {
            writeCopyData(new byte[]{'m', 1, 2, 3});
        }

        /** 读到客户端关闭为止（会话收尾——心跳写帧被静默消费直到 EOF）。 */
        void drainUntilClientClose() {
            OgPseudoServer.drainQuietly(in);
        }

        /** 主动断开当前连接（断线重连场景的触发器——静默清理）。 */
        void closeClient() {
            closeClientQuietly();
        }

        void closeClientQuietly() {
            try {
                socket.close();
            } catch (IOException e) {
                // 清理路径不抛
            }
        }

        private static byte[] int32(int v) {
            return new byte[]{(byte) (v >> 24), (byte) (v >> 16), (byte) (v >> 8), (byte) v};
        }

        private static void putU64(byte[] buf, int off, long v) {
            for (int i = 0; i < 8; i++) {
                buf[off + i] = (byte) (v >>> (8 * i));
            }
        }

        private static void putI32(byte[] buf, int off, int v) {
            for (int i = 0; i < 4; i++) {
                buf[off + i] = (byte) (v >>> (8 * i));
            }
        }
    }

    /**
     * 65B StandbyReplyMessage 的解码视图（小端），供伪服务端断言客户端反馈字段。
     *
     * @param receive       已收前沿 u64
     * @param write         已写前沿 u64
     * @param flush         已刷前沿 u64
     * @param apply         已应用前沿 u64
     * @param applyRead     applyRead u64
     * @param peerRole      本端角色 i32（STANDBY_MODE=3）
     * @param peerState     本端状态 i32（BUILDING_STATE=7）
     * @param replyRequested 即答请求位 u8
     */
    record StandbyReply(long receive, long write, long flush, long apply, long applyRead,
                        int peerRole, int peerState, long sendTime, byte replyRequested) {

        static StandbyReply decode(byte[] frame, int off) {
            return new StandbyReply(u64(frame, off + 1), u64(frame, off + 9), u64(frame, off + 17),
                    u64(frame, off + 25), u64(frame, off + 33), i32(frame, off + 41), i32(frame, off + 45),
                    u64(frame, off + 49), frame[off + 57]);
        }

        private static long u64(byte[] b, int off) {
            long v = 0;
            for (int i = 7; i >= 0; i--) {
                v = (v << 8) | (b[off + i] & 0xFFL);
            }
            return v;
        }

        private static int i32(byte[] b, int off) {
            return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16)
                    | ((b[off + 3] & 0xFF) << 24);
        }
    }

    /** 静默丢帧辅助（EOF 视为客户端正常关闭）。 */
    static void drainQuietly(InputStream in) {
        try {
            while (in.read() != -1) {
                // 排空
            }
        } catch (IOException e) {
            // 对端已关——预期
        }
    }
}
