import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Offline-replay spike 的宿主 SQL 工具：读 SQL 脚本按 ';' 切分逐句执行（脚本不含
 * 过程体/函数体，切分安全）。走 opengauss-jdbc（OG 容器 hba=md5 + sha256 存储 →
 * 认证码 11，vanilla pgjdbc 处理不了）。
 */
public class SqlRunner {
    public static void main(String[] args) throws Exception {
        // 参数化形态（VastBase 真库验证）：<url> <user> <pass> <sqlFile>；单参形态保留 OG 容器缺省
        String url = args.length >= 4 ? args[0] : "jdbc:opengauss://localhost:55440/postgres";
        String user = args.length >= 4 ? args[1] : "gaussdb";
        String pass = args.length >= 4 ? args[2] : "OgTest#2026";
        String sqlFile = args.length >= 4 ? args[3] : args[0];
        String sql = Files.readString(Path.of(sqlFile), StandardCharsets.UTF_8);
        try (Connection c = DriverManager.getConnection(url, user, pass);
             Statement st = c.createStatement()) {
            for (String raw : sql.split(";")) {
                StringBuilder sb = new StringBuilder();
                for (String line : raw.split("\n")) {
                    if (!line.trim().startsWith("--")) {
                        sb.append(line).append("\n");
                    }
                }
                String stmt = sb.toString().trim();
                if (stmt.isEmpty()) {
                    continue;
                }
                boolean hasRs = st.execute(stmt);
                String head = stmt.substring(0, Math.min(70, stmt.length())).replace("\n", " ");
                if (hasRs) {
                    try (java.sql.ResultSet rs = st.getResultSet()) {
                        StringBuilder out = new StringBuilder("OK: " + head + " =>");
                        while (rs.next()) {
                            int n = rs.getMetaData().getColumnCount();
                            out.append(" [");
                            for (int i = 1; i <= n; i++) {
                                out.append(rs.getString(i)).append(i < n ? " | " : "");
                            }
                            out.append("]");
                        }
                        System.out.println(out);
                    }
                } else {
                    System.out.println("OK: " + head);
                }
            }
        }
    }
}
