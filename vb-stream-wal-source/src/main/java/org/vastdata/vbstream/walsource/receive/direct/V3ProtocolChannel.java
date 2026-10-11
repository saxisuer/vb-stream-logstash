package org.vastdata.vbstream.walsource.receive.direct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * 裸 PostgreSQL v3 协议通道（openGauss 物理复制专用，Task 5）：socket + startup +
 * openGauss 认证（md5/sha256 家族）+ 'Q' 简单查询命令 + CopyBoth 复制帧读写。pgjdbc /
 * opengauss-jdbc 的 replication API 在 openGauss 上不可用（{@code replication=database}
 * 形态被服务端硬路由进逻辑发送循环），合法物理流只有 {@code replication=true}（startup
 * 不带真实库名、database 固定 "replication"——pg_receivexlog 的 streamutil.cpp:175-176
 * 形态）+ 'Q' 命令 + CopyBoth + 65B 扩展 'r' 反馈，本类是该协议的纯客户端实现。
 *
 * <p><strong>线格式事实锚</strong>（openGauss 5.0.0 源码 /tmp/og-src 转录）：</p>
 * <ul>
 *   <li>FE/BE 帧（startup 之后）：类型 char + int32 大端长度（含长度自身 4B）+ 载荷。</li>
 *   <li>复制消息（CopyData 'd' 载荷内）为 C 结构体 memcpy——<b>小端、含尾部对齐垫</b>：
 *       'w' 帧头 = {@code WalDataMessageHeader}（walprotocol.h:103——dataStart u64@0/
 *       walEnd u64@8/sendTime i64@16/四 sender 位 u64@24-56/catchup u8@56 + 垫，
 *       sizeof=64，WAL 载荷自帧偏移 65 起；walsender.cpp:5217 XLogRead 同偏移锚）；
 *       'k' 帧 = {@code WalSndrMessage}（walprotocol.h:45，sizeof=32，含 replyRequested
 *       u8@24）——receivelog.cpp:46-47 的 STREAMING_HEADER_SIZE=65/KEEPALIVE_SIZE=33
 *       即 sizeof+1，结构体垫上传输的实证。</li>
 *   <li>65B 'r' 反馈 = {@code StandbyReplyMessage}（walprotocol.h:201——receive/write/
 *       flush/apply/applyRead 各 u64 + peer_role i32 + peer_state i32 + sendTime i64 +
 *       replyRequested u8 + 垫 3 + replyFlags u32，sizeof=64）加首字节 'r'；字段值
 *       peer_role=STANDBY_MODE=3、peer_state=BUILDING_STATE=7（replicainternal.h:62-85
 *       枚举实值——brief 所记 2/4 与头文件不符，以头文件为准）、sendTime = 自
 *       2000-01-01 的微秒 i64（timestamp.h integer_datetimes）。</li>
 *   <li>认证：'R' 码 5（md5，4B 盐）/ 码 10（openGauss 扩展，载荷 = i32 子码 + 64 字符
 *       盐 + 8 字符 token + 64 字符 sever_signature[3.0 形态] 或 i32 迭代数[3.51 形态]）
 *       / 码 11（64 字符 token + 4B 盐）——算法面见 {@link OgAuth}（码 11 形态与迭代数
 *       缺省以真容器 oracle 实锚修正，opengauss-jdbc 实现在两处与 5.0.0 服务端不符）。</li>
 * </ul>
 *
 * <p>线程约束：读方法（readCopyData 等）单读线程独占；写方法（sendCopyData/
 * sendStandbyReply）经 {@code synchronized(sendLock)} 互斥——心跳线程与读线程的
 * keepalive 即时回应可并发写同一通道。非线程安全的是"读写交替的命令面"（connect/
 * identifySystem/startPhysicalReplication 须单线程串行）。</p>
 */
public final class V3ProtocolChannel implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(V3ProtocolChannel.class);

    /** AuthenticationMD5Password（标准 PG md5 挑战）。 */
    public static final int AUTH_REQ_MD5 = 5;

    /** openGauss sha256 挑战（子码区分服务端口令存储形态）。 */
    public static final int AUTH_REQ_SHA256 = 10;

    /** openGauss 混合形态挑战（服务端存 sha256、md5 通道交换）。 */
    public static final int AUTH_REQ_MD5_SHA256 = 11;

    /** 'w' 帧头宽（sizeof(WalDataMessageHeader)，含尾部对齐垫）——WAL 载荷在帧偏移 1+64。 */
    public static final int WAL_HEADER_SIZE = 64;

    /**
     * sha256 挑战迭代数缺省——openGauss 服务端缺省 10000（sha2.h:53 ITERATION_COUNT；3.0
     * 形态挑战以 server_signature 收尾不携带迭代数，3.51 形态显式携带 i32）。
     */
    private static final int DEFAULT_SHA256_ITERATIONS = OgAuth.SERVER_DEFAULT_ITERATIONS;

    /** 单帧载荷上限（64MB 帧间防线——16MB 段 + 头远在其下，超限视为协议错位 fail-fast）。 */
    private static final int MAX_FRAME_PAYLOAD = 64 * 1024 * 1024;

    /** 'r' 反馈总长（'r' + sizeof(StandbyReplyMessage)）——黄金布局见 encodeStandbyReply。 */
    public static final int STANDBY_REPLY_SIZE = 65;

    private final String host;
    private final int port;
    private final String user;
    private final String password;
    private final String applicationName;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private final Object sendLock = new Object();

    private Socket socket;
    private DataInputStream in;
    private OutputStream out;

    /** 通道是否已进入 CopyBoth 流面（readCopyData 的前置状态）。 */
    private boolean copyMode;

    /**
     * 装配通道参数（不连接——connect() 才碰网络）。
     *
     * @param host           服务端主机
     * @param port           服务端端口
     * @param user           用户（须有 REPLICATION 权限）
     * @param password       密码
     * @param applicationName startup 的 application_name（观测面标识）
     * @param connectTimeoutMs 连接超时（毫秒，&le;0 走系统缺省）
     * @param readTimeoutMs   读超时（毫秒，&le;0 无限阻塞——超时抛 SocketTimeoutException
     *                        视为断流由上层重连）
     */
    public V3ProtocolChannel(String host, int port, String user, String password,
                             String applicationName, int connectTimeoutMs, int readTimeoutMs) {
        this.host = host;
        this.port = port;
        this.user = user;
        this.password = password;
        this.applicationName = applicationName;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    /**
     * 建连 + startup + openGauss 认证 + 消费至 ReadyForQuery。
     *
     * <p>关键步骤：TCP 连接（回环外场景的 connect 超时）→ 发 startup（协议 3.0、
     * user/database=replication/replication=true/application_name，
     * streamutil.cpp:175-176 形态）→ 认证循环（见
     * {@link #authenticate}，直到 AuthenticationOk）→ 消费 ParameterStatus/
     * BackendKeyData/Notice 至 'Z'。任何阶段收到 ErrorResponse 抛 IOException（消息含
     * 服务端文本）；认证成功后的 'S' 参数表写入 serverParams 观测面。</p>
     *
     * @throws IOException 连接/认证/协议错位失败
     */
    public void connect() throws IOException {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
        socket.setTcpNoDelay(true);
        if (readTimeoutMs > 0) {
            socket.setSoTimeout(readTimeoutMs);
        }
        in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out = new BufferedOutputStream(socket.getOutputStream());
        sendStartup();
        authenticate();
        awaitReadyForQuery();
        LOG.debug("v3 通道已建立: {}:{} user={} (application_name={})", host, port, user, applicationName);
    }

    /** 服务端 ParameterStatus 表（认证后观测面，readStartup 阶段逐条累积）。 */
    private final Map<String, String> serverParams = new HashMap<>();

    /**
     * 认证后的 ParameterStatus 观测面（如 server_version）。
     *
     * @return 参数表快照（可能为空——服务端未发即空）
     */
    public Map<String, String> serverParameters() {
        return Map.copyOf(serverParams);
    }

    /**
     * 发 startup 消息：int16 大版本 3 + int16 小版本 0 + k\0v\0 参数对 + 终止 \0，
     * 整体以 int32 总长前缀（含自身）包裹——FE/BE 协议 v3 唯一的无类型字节消息。
     *
     * <p>参数面注意：{@code fallback_application_name} 是 libpq 客户端专有参数（libpq
     * 在 application_name 已设时直接剥除、不上线——openGauss 5.0.0 服务端对其报
     * FATAL 42704 unrecognized configuration parameter，实测锚），故本客户端只发
     * {@code application_name}。</p>
     */
    private void sendStartup() throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("user", user);
        params.put("database", "replication");
        params.put("replication", "true");
        params.put("application_name", applicationName);
        StringBuilder body = new StringBuilder();
        for (Map.Entry<String, String> e : params.entrySet()) {
            body.append(e.getKey()).append('\0').append(e.getValue()).append('\0');
        }
        body.append('\0');
        byte[] bodyBytes = body.toString().getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + 4 + bodyBytes.length);
        buf.putInt(4 + 4 + bodyBytes.length);
        buf.putShort((short) 3);
        buf.putShort((short) 0);
        buf.put(bodyBytes);
        out.write(buf.array());
        out.flush();
    }

    /**
     * 认证循环：逐帧读 'R'（Authentication*）/'S'/'K'/'N'/'v' 直到 AuthenticationOk。
     *
     * <p>关键步骤：'R' 载荷首 i32 为认证码——码 0 认证通过返回；码 5 读 4B 盐回 md5 摘要；
     * 码 10 读 i32 子码（子码 1 = 服务端存 md5：4B 盐回 sha256Md5；子码 0/2 = sha256
     * 挑战：64 字符盐 + 8 字符 token [+ i32 迭代数]，回 RFC5802 proof）；码 11 读 64
     * 字符 token + 4B 盐回 md5Sha256。应答统一走 PasswordMessage（'p' + 摘要 + NUL）。
     * 其余认证码抛 IOException（不支持面 fail-fast）。</p>
     *
     * @throws IOException 认证失败/不支持/ErrorResponse
     */
    private void authenticate() throws IOException {
        while (true) {
            int type = readMessageType();
            int len = readInt32();
            byte[] payload = readFully(len - 4);
            switch (type) {
                case 'R' -> {
                    ByteBuffer body = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
                    int code = body.getInt();
                    if (code == 0) {
                        return;   // AuthenticationOk
                    }
                    byte[] digest = switch (code) {
                        case AUTH_REQ_MD5 -> {
                            byte[] salt = new byte[4];
                            body.get(salt);
                            yield OgAuth.md5Password(user.getBytes(StandardCharsets.UTF_8),
                                    password.getBytes(StandardCharsets.UTF_8), salt);
                        }
                        case AUTH_REQ_SHA256 -> handleSha256Challenge(body);
                        case AUTH_REQ_MD5_SHA256 -> {
                            byte[] token = readFixedChars(body, 64);
                            byte[] salt = new byte[4];
                            body.get(salt);
                            yield OgAuth.md5Sha256Password(password,
                                    new String(token, StandardCharsets.UTF_8), salt);
                        }
                        default -> throw new IOException("不支持的认证请求码 " + code
                                + "（支持面: 0/5/10/11）");
                    };
                    sendPasswordMessage(digest);
                }
                case 'E' -> throw asError(payload);
                case 'N', 'S', 'K', 'v' -> skipPayload(type);
                default -> throw new IOException("认证阶段收到意外消息类型 '" + (char) type + "'");
            }
        }
    }

    /**
     * 处理码 10 挑战的子码分派（opengauss-jdbc doAuthentication 码 10 分支 + auth.cpp
     * 挑战构造的协议版本分叉转录）。
     *
     * <p>载荷形态（子码 0/2 后）：64 字符盐 + 8 字符 token，其后按客户端协议版本分叉——
     * <b>3.0</b>（minor &lt; GAUSS_BASE）：64 字符 sever_signature（服务端身份校验用，
     * 迭代数不携带，回落服务端缺省 10000）；<b>3.51+</b>：i32 迭代数。子码 1 = 服务端存
     * md5（4B 盐 → sha256Md5 应答）。</p>
     *
     * @param body 'R' 载荷（已剥认证码，定位在子码）
     * @return 客户端应答摘要
     * @throws IOException 子码不支持或载荷形态异常
     */
    private byte[] handleSha256Challenge(ByteBuffer body) throws IOException {
        int subtype = body.getInt();
        if (subtype == 1) {
            byte[] salt = new byte[4];
            body.get(salt);
            return OgAuth.sha256Md5Password(user.getBytes(StandardCharsets.UTF_8),
                    password.getBytes(StandardCharsets.UTF_8), salt);
        }
        if (subtype != 0 && subtype != 2) {
            throw new IOException("不支持的 sha256 认证子码 " + subtype + "（支持面: 0/1/2）");
        }
        byte[] saltHex = readFixedChars(body, 64);
        byte[] tokenHex = readFixedChars(body, 8);
        String salt = new String(saltHex, StandardCharsets.UTF_8);
        String token = new String(tokenHex, StandardCharsets.UTF_8);
        if (body.remaining() >= 64) {
            byte[] signature = readFixedChars(body, 64);
            try {
                return OgAuth.rfc5802ProofVerified(password, salt, token,
                        DEFAULT_SHA256_ITERATIONS, new String(signature, StandardCharsets.UTF_8));
            } catch (IllegalStateException e) {
                throw new IOException("sha256 挑战服务端签名校验失败: " + e.getMessage(), e);
            }
        }
        int iterations = body.remaining() >= 4 ? body.getInt() : DEFAULT_SHA256_ITERATIONS;
        return OgAuth.rfc5802Proof(password, salt, token, iterations);
    }

    /**
     * 从认证载荷读定宽 ASCII 段（伪 NUL 终止的定宽 token——与 opengauss-jdbc
     * receiveString(n) 的固定 n 字节读同形）。
     */
    private static byte[] readFixedChars(ByteBuffer body, int n) throws IOException {
        if (body.remaining() < n) {
            throw new IOException("认证载荷不足：还需 " + n + " 字节，实剩 " + body.remaining());
        }
        byte[] out = new byte[n];
        body.get(out);
        return out;
    }

    /**
     * 发 PasswordMessage（'p' + int32 长度 + 摘要 + NUL——所有认证应答的统一回帧）。
     */
    private void sendPasswordMessage(byte[] digest) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + digest.length + 1);
        buf.put((byte) 'p');
        buf.putInt(4 + digest.length + 1);
        buf.put(digest);
        buf.put((byte) 0);
        synchronized (sendLock) {
            out.write(buf.array());
            out.flush();
        }
    }

    /**
     * 认证通过后消费至 ReadyForQuery（'Z'）——逐帧跳 'S'（记录观测表）/'K'/'N'/'v'。
     */
    private void awaitReadyForQuery() throws IOException {
        while (true) {
            int type = readMessageType();
            int len = readInt32();
            byte[] payload = readFully(len - 4);
            switch (type) {
                case 'Z' -> {
                    return;
                }
                case 'S' -> {
                    int zero1 = indexOf(payload, 0);
                    serverParams.put(new String(payload, 0, zero1, StandardCharsets.UTF_8),
                            new String(payload, zero1 + 1, payload.length - zero1 - 2, StandardCharsets.UTF_8));
                }
                case 'E' -> throw asError(payload);
                case 'K', 'N', 'v' -> {
                    // BackendKeyData/Notice/NegotiateProtocolVersion——本客户端不消费
                }
                default -> throw new IOException("等 ReadyForQuery 时收到意外消息类型 '" + (char) type + "'");
            }
        }
    }

    /**
     * 'Q' 简单查询 IDENTIFY_SYSTEM 并解析 DataRow 字段（systemid/timeline/xlogpos/…）。
     *
     * <p>关键步骤：发 'Q' → 逐帧读至 ReadyForQuery：'T' RowDescription 跳过、'D' DataRow
     * 取首行字段（int16 列数 + 逐列 int32 长度前缀[0 长度=空串、-1=NULL→空串]文本）、
     * 'C' CommandComplete 记录、'E' 上抛。行数 &ne; 1 抛 IOException（协议错位 fail-fast，
     * receivelog.cpp:597 对 PGRES_TUPLES_OK + 单行的校验同构）。</p>
     *
     * @return DataRow 字段文本（列序即服务端列序）
     * @throws IOException ErrorResponse/协议错位
     */
    public String[] identifySystem() throws IOException {
        String[] row = null;
        int rows = 0;
        sendQuery("IDENTIFY_SYSTEM");
        while (true) {
            int type = readMessageType();
            int len = readInt32();
            byte[] payload = readFully(len - 4);
            switch (type) {
                case 'T', 'C' -> {
                    // 列描述与完成 tag——不解析
                }
                case 'D' -> {
                    rows++;
                    row = parseDataRow(payload);
                }
                case 'Z' -> {
                    if (rows != 1) {
                        throw new IOException("IDENTIFY_SYSTEM 期望单行 DataRow，实得 " + rows + " 行");
                    }
                    return row;
                }
                case 'E' -> throw asError(payload);
                case 'N', 'S', 'K' -> {
                    // Notice/参数/键数据——跳过
                }
                default -> throw new IOException("IDENTIFY_SYSTEM 应答中收到意外消息类型 '" + (char) type + "'");
            }
        }
    }

    /**
     * 'Q' 发 START_REPLICATION（物理流）并期待 CopyBothResponse（'W'）。
     *
     * <p>命令文本 = {@code START_REPLICATION [SLOT "name"] HI/LO}（receivelog.cpp:632 +
     * slotcmd 拼接形态）；收到 'W' 即置 copyMode 返回（此后 readCopyData 读流面）。
     * ErrorResponse（槽不存在等）上抛 IOException。</p>
     *
     * @param slotName 复制槽名（null = 无槽裸流）
     * @param startLsn 流起点（绝对 LSN——段对齐由调用方负责）
     * @throws IOException ErrorResponse/意外消息
     */
    public void startPhysicalReplication(String slotName, long startLsn) throws IOException {
        // LSN 文本取 pg_receivexlog 的 %X/%X 非补零形态（receivelog.cpp:632）——低段不补零，
        // 与模块日志面的 Lsn.format(%08X) 有意分叉（服务端 sscanf 两者同收，向参考客户端对齐）。
        String sql = "START_REPLICATION "
                + (slotName != null ? "SLOT \"" + slotName + "\" " : "")
                + String.format("%X/%X", startLsn >>> 32, startLsn & 0xFFFFFFFFL);
        sendQuery(sql);
        while (true) {
            int type = readMessageType();
            int len = readInt32();
            byte[] payload = readFully(len - 4);
            switch (type) {
                case 'W' -> {
                    copyMode = true;
                    LOG.info("START_REPLICATION 进入 CopyBoth 流面: {} 起点 {}", sql, Lsn.format(startLsn));
                    return;
                }
                case 'E' -> throw asError(payload);
                case 'N' -> {
                    // Notice——跳过
                }
                case 'T', 'D', 'C', 'Z' -> throw new IOException("START_REPLICATION 未进入 CopyBoth——"
                        + "收到 '" + (char) type + "'（起点越界/槽状态异常？）");
                default -> throw new IOException("START_REPLICATION 应答中收到意外消息类型 '" + (char) type + "'");
            }
        }
    }

    /**
     * 读下一条 CopyData 载荷（阻塞——CopyBoth 流面）。
     *
     * <p>返回值首字节为复制消息类型（'w'/'k'/'m'/…），其后为小端结构体与 WAL 字节；
     * 帧间可夹 'N'/'S'/'A'（跳过）。CopyDone（'c'）返回 null（服务端正常收流），
     * CopyFail/ErrorResponse 抛 IOException。'w' 帧解析辅助见 {@link #walDataStart} 与
     * {@link #walPayload}。</p>
     *
     * @return CopyData 载荷；CopyDone 为 null
     * @throws IOException 流断/协议错位/ErrorResponse
     */
    public byte[] readCopyData() throws IOException {
        if (!copyMode) {
            throw new IllegalStateException("未进入 CopyBoth 流面（startPhysicalReplication 先行）");
        }
        while (true) {
            int type = readMessageType();
            int len = readInt32();
            byte[] payload = readFully(len - 4);
            switch (type) {
                case 'd' -> {
                    return payload;
                }
                case 'c' -> {
                    return null;
                }
                case 'f' -> throw new IOException("服务端 CopyFail: " + new String(payload, StandardCharsets.UTF_8));
                case 'E' -> throw asError(payload);
                case 'N', 'S', 'A' -> {
                    // Notice/参数/通知——流面夹带，跳过
                }
                default -> throw new IOException("CopyBoth 流面收到意外消息类型 '" + (char) type + "'");
            }
        }
    }

    /**
     * 'w' 帧的 dataStart（本帧 WAL 载荷的起始 LSN——帧偏移 1 的 u64 小端）。
     *
     * @param frame readCopyData 返回的 'w' 帧
     * @return dataStart
     */
    public static long walDataStart(byte[] frame) {
        return structU64(frame, 1);
    }

    /**
     * 'w' 帧的 walEnd（发送端 WAL 末——帧偏移 9 的 u64 小端，观测面）。
     *
     * @param frame readCopyData 返回的 'w' 帧
     * @return walEnd
     */
    public static long walEnd(byte[] frame) {
        return structU64(frame, 9);
    }

    /**
     * 'w' 帧的 WAL 载荷（64B 头之后的字节——不拷贝，原帧切片视图语义的独立数组）。
     *
     * @param frame readCopyData 返回的 'w' 帧
     * @return 载荷字节（帧长恰为头时为空数组）
     */
    public static byte[] walPayload(byte[] frame) {
        byte[] payload = new byte[frame.length - 1 - WAL_HEADER_SIZE];
        System.arraycopy(frame, 1 + WAL_HEADER_SIZE, payload, 0, payload.length);
        return payload;
    }

    /**
     * 'k' 帧的 walEnd（发送端 WAL 末——帧偏移 1 的 u64 小端，观测面）。
     *
     * <p>前置：帧长恰 33（sizeof(WalSndrMessage)+1）——调用方先行校验，本方法不再越界
     * 防护（畸形帧由调用方按协议错位上抛，见 DirectWalSource.streamLoop）。</p>
     *
     * @param frame readCopyData 返回的 'k' 帧（长度 33）
     * @return walEnd
     */
    public static long walEndOfKeepalive(byte[] frame) {
        return structU64(frame, 1);
    }

    /**
     * 'k' 帧的 replyRequested 位（帧偏移 25 的 u8——置位须即时回 'r'）。
     *
     * <p>前置：帧长恰 33——调用方先行校验（畸形短帧取本位会 AIOOBE，须按协议错位的
     * 可重试面处理，见 DirectWalSource.streamLoop）。</p>
     *
     * @param frame readCopyData 返回的 'k' 帧（长度 33）
     * @return 即答请求为 true
     */
    public static boolean keepaliveReplyRequested(byte[] frame) {
        return frame[25] != 0;
    }

    /**
     * 编码 65B StandbyReplyMessage（'r' 反馈，小端——布局与 walprotocol.h:201 结构体
     * memcpy 一致，含 C 对齐垫；C 参考实现在 receivelog.cpp:372 sendReplyToSender）：
     * {@code 'r' + receive/write/flush/apply/applyRead 各 u64 + peer_role i32(=3
     * STANDBY_MODE) + peer_state i32(=7 BUILDING_STATE) + sendTime i64(µs 自
     * 2000-01-01) + replyRequested u8 + 垫 3 + replyFlags u32(=0)}。
     *
     * @param receive   已收前沿
     * @param write     已写前沿
     * @param flush     已刷前沿
     * @param apply     已应用前沿
     * @param applyRead applyRead 前沿
     * @param sendTimeMicros 发送时刻（自 2000-01-01 的微秒）
     * @param replyRequested 即答请求位（心跳置 true、keepalive 即时回应置 false——
     *                       receivelog.cpp:789 形态）
     * @return 65 字节反馈帧（CopyData 载荷形态，含首字节 'r'）
     */
    public static byte[] encodeStandbyReply(long receive, long write, long flush, long apply,
                                            long applyRead, long sendTimeMicros, boolean replyRequested) {
        ByteBuffer buf = ByteBuffer.allocate(STANDBY_REPLY_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) 'r');
        buf.putLong(receive);
        buf.putLong(write);
        buf.putLong(flush);
        buf.putLong(apply);
        buf.putLong(applyRead);
        buf.putInt(3);   // peer_role = STANDBY_MODE（replicainternal.h ServerMode 枚举序）
        buf.putInt(7);   // peer_state = BUILDING_STATE（replicainternal.h DbState 枚举序）
        buf.putLong(sendTimeMicros);
        buf.put((byte) (replyRequested ? 1 : 0));
        buf.put(new byte[3]);   // replyRequested → replyFlags 的 C 结构体对齐垫
        buf.putInt(0);          // replyFlags（IS_PAUSE_BY_TARGET_BARRIER 等——裸接收端恒 0）
        return buf.array();
    }

    /**
     * 发 'r' 反馈（CopyData 包裹，四前沿取同一消费前沿值，sendTime 取当前时刻）——
     * 心跳线程与 keepalive 即时回应共用；与其它写帧经 sendLock 互斥防交错。
     *
     * @param frontier        消费前沿（receive/write/flush 三位同报）
     * @param replyRequested 即答请求位
     * @throws IOException 写失败（断流信号——上层重连）
     */
    public void sendStandbyReply(long frontier, boolean replyRequested) throws IOException {
        long micros = (System.currentTimeMillis() - 946_684_800_000L) * 1000;
        sendCopyData(encodeStandbyReply(frontier, frontier, frontier, 0, frontier,
                micros, replyRequested));
    }

    /**
     * 发 CopyData 帧（'d' + int32 长度 + 载荷）。
     *
     * @param payload 复制消息载荷（首字节 'r' 等）
     * @throws IOException 写失败
     */
    public void sendCopyData(byte[] payload) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + payload.length);
        buf.put((byte) 'd');
        buf.putInt(4 + payload.length);
        buf.put(payload);
        synchronized (sendLock) {
            out.write(buf.array());
            out.flush();
        }
    }

    /**
     * 'Q' 简单查询发送（SQL + NUL）。
     */
    private void sendQuery(String sql) throws IOException {
        byte[] sqlBytes = sql.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + sqlBytes.length + 1);
        buf.put((byte) 'Q');
        buf.putInt(4 + sqlBytes.length + 1);
        buf.put(sqlBytes);
        buf.put((byte) 0);
        out.write(buf.array());
        out.flush();
    }

    /**
     * 关通道（幂等——半开连接的写侧 shutdown 让对端读侧尽速感知）。
     */
    @Override
    public void close() {
        Socket s = socket;
        if (s != null && !s.isClosed()) {
            try {
                s.close();
            } catch (IOException e) {
                LOG.debug("关 v3 通道失败（忽略）: {}", e.getMessage());
            }
        }
    }

    // ---- 帧读原语 ----

    /**
     * 读帧类型字节（阻塞）。EOF 抛 EOFException——断流的上抛面。
     */
    private int readMessageType() throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new EOFException("服务端关闭连接（读帧类型位时 EOF）");
        }
        return b;
    }

    /**
     * 读 int32 帧长（大端）并做上限防线。
     */
    private int readInt32() throws IOException {
        int len = in.readInt();
        if (len < 4 || len - 4 > MAX_FRAME_PAYLOAD) {
            throw new IOException("帧长越界: " + len + "（协议错位 fail-fast）");
        }
        return len;
    }

    /**
     * 全量读 n 字节载荷（DataInputStream.readFully 语义）。
     */
    private byte[] readFully(int n) throws IOException {
        byte[] payload = new byte[n];
        in.readFully(payload);
        return payload;
    }

    /**
     * DataRow 载荷解析：int16 列数 + 逐列 int32 长度前缀文本值。
     */
    private static String[] parseDataRow(byte[] payload) {
        ByteBuffer body = ByteBuffer.wrap(payload).order(ByteOrder.BIG_ENDIAN);
        int cols = body.getShort();
        String[] fields = new String[cols];
        for (int i = 0; i < cols; i++) {
            int len = body.getInt();
            if (len < 0) {
                fields[i] = "";   // NULL 列降级空串（IDENTIFY_SYSTEM 面无 NULL，防御位）
                continue;
            }
            byte[] v = new byte[len];
            body.get(v);
            fields[i] = new String(v, StandardCharsets.UTF_8);
        }
        return fields;
    }

    /**
     * ErrorResponse 载荷转 IOException（M/C 字段入消息——排障可读性优先）。
     */
    private static IOException asError(byte[] payload) {
        String severity = "";
        String code = "";
        String message = "";
        int i = 0;
        while (i < payload.length && payload[i] != 0) {
            char fieldType = (char) payload[i];
            int end = i + 1;
            while (end < payload.length && payload[end] != 0) {
                end++;
            }
            String value = new String(payload, i + 1, end - i - 1, StandardCharsets.UTF_8);
            switch (fieldType) {
                case 'S' -> severity = value;
                case 'C' -> code = value;
                case 'M' -> message = value;
                default -> {
                    // 其余字段（D/H/P/F/L/R 等）不消费
                }
            }
            i = end + 1;
        }
        return new IOException("服务端 ErrorResponse: [" + severity + "/" + code + "] " + message);
    }

    /**
     * 认证阶段对可跳过帧（'N'/'S'/'K'/'v'）的容错消费——载荷已读尽丢弃（认证成功后的
     * 'S' 参数面由 awaitReadyForQuery 正式记录）。
     *
     * @param type 被跳过的消息类型字符（DEBUG 观测面）
     */
    private static void skipPayload(int type) {
        LOG.debug("认证阶段跳过消息 '{}'", (char) type);
    }

    /**
     * 小端 u64 结构体字段读（'w'/'k' 帧 memcpy 布局的解面）。
     */
    private static long structU64(byte[] frame, int off) {
        long v = 0;
        for (int i = 7; i >= 0; i--) {
            v = (v << 8) | (frame[off + i] & 0xFFL);
        }
        return v;
    }

    /**
     * 载荷内查 NUL 分隔位（ParameterStatus 键值切分）。
     */
    private static int indexOf(byte[] buf, int b) {
        for (int i = 0; i < buf.length; i++) {
            if (buf[i] == b) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 通道是否处于 CopyBoth 流面（观测面——重连装配的判据）。
     *
     * @return 已进入 CopyBoth 为 true
     */
    public boolean isCopyMode() {
        return copyMode;
    }
}
