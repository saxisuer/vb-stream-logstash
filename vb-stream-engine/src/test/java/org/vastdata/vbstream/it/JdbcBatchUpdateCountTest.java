package org.vastdata.vbstream.it;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 * pgjdbc executeBatch() 对同一行连续 UPDATE 的计数语义探针（Testcontainers 真 PG 18，需本机 Docker）。
 *
 * <p>要回答的问题：同一行在一个 PreparedStatement batch 里更新 3 次，executeBatch() 返回
 * [1,1,1] 还是 [1,0,0]。两个场景划出分界——变值更新（基线）与同值更新（[1,0,0] 候选唯一的
 * 可能来源：MySQL 驱动默认报 changed rows，同值 UPDATE 第二次起返回 0）。PostgreSQL 的
 * UPDATE 命令标记按 matched rows 计数，预期两场景均为 [1,1,1]，本类实证之。
 */
class JdbcBatchUpdateCountTest {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcBatchUpdateCountTest.class);

    /** it 包容器跨测试类共享，表名固定为类私有名并在前后各做一次建/删，避免残留。 */
    @BeforeEach
    void setUpTable() throws SQLException {
        PgTestEnv.execSql(
                "DROP TABLE IF EXISTS it_jdbc_batch_count",
                "CREATE TABLE it_jdbc_batch_count(id int PRIMARY KEY, v int)",
                "INSERT INTO it_jdbc_batch_count VALUES (1, 0)");
    }

    @AfterEach
    void dropTable() throws SQLException {
        PgTestEnv.execSql("DROP TABLE IF EXISTS it_jdbc_batch_count");
    }

    @Test
    void sameRowDistinctValueBatchUpdatesAllReportOne() throws SQLException {
        int[] counts = batchUpdateSameRow(11, 22, 33);
        LOG.info("同一行 batch 变值更新 3 次（11/22/33）的 executeBatch 计数: {}", Arrays.toString(counts));
        assertArrayEquals(new int[]{1, 1, 1}, counts,
                "变值更新每条 UPDATE 都匹配同一行，matched 计数应逐条为 1");
    }

    @Test
    void sameRowIdenticalValueBatchUpdatesAllReportOne() throws SQLException {
        int[] counts = batchUpdateSameRow(7, 7, 7);
        LOG.info("同一行 batch 同值更新 3 次（7/7/7）的 executeBatch 计数: {}", Arrays.toString(counts));
        assertArrayEquals(new int[]{1, 1, 1}, counts,
                "PG 计数语义是 matched rows 而非 changed rows，同值更新第二次起不应归零");
    }

    /**
     * 对 id=1 的同一行以给定值序列做一次 PreparedStatement batch 更新，返回 executeBatch() 的计数数组。
     *
     * <p>关键步骤：单连接（autocommit）内同一语句模板 "UPDATE ... SET v=? WHERE id=1"
     * 逐值绑定 addBatch，一次 executeBatch() 收口——pgjdbc 将 batch 打包为扩展协议流水线
     * 逐条执行，每条各自带回 CommandComplete 命令标记解析出的行计数。
     *
     * <p>边界：行不存在时对应位返回 0（本用例 @BeforeEach 恒插有 id=1，不会触发）；
     * 数组长度与 values 一致由协议保证，无需额外校验。
     *
     * @param values 依序绑定到各条 batch 语句的 v 值（值是否互异由调用场景决定）
     * @return executeBatch() 原样返回的计数数组（SUCCESS_NO_INFO=-2 不在本用例路径出现）
     */
    private int[] batchUpdateSameRow(int... values) throws SQLException {
        try (PreparedStatement ps = PgTestEnv.newSqlConnection()
                .prepareStatement("UPDATE it_jdbc_batch_count SET v = ? WHERE id = 1")) {
            for (int v : values) {
                ps.setInt(1, v);
                ps.addBatch();
            }
            return ps.executeBatch();
        }
    }
}
