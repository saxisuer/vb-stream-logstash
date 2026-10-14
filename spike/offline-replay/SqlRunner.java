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
        String url = "jdbc:opengauss://localhost:55440/postgres";
        String sql = Files.readString(Path.of(args[0]), StandardCharsets.UTF_8);
        try (Connection c = DriverManager.getConnection(url, "gaussdb", "OgTest#2026");
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
                st.execute(stmt);
                String head = stmt.substring(0, Math.min(70, stmt.length())).replace("\n", " ");
                System.out.println("OK: " + head);
            }
        }
    }
}
