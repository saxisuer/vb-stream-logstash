package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * 责任:模块自含性边界钉死——扫描 target/classes 下全部常规文件(覆盖主源码与资源两面),
 * 断言无任何 io/debezium/connector/postgresql 引用(vanilla 连接器类不得回流)。
 * 边界:仅静态扫描已编译产物,不运行期加载;needle 对二进制类文件(字节码常量池的
 * 斜杠形态包名)与文本资源(如 META-INF/services 清单)同样适用;缺 target/classes
 * (未编译直接跑)时先由 surefire 的 test-compile 阶段保证存在,找不到即 fail。
 * 线程约束:无共享状态。
 */
class VanillaBoundaryTest {

    @Test
    void noFileReferencesVanillaPostgresConnectorPackage() throws IOException {
        Path classes = Path.of("target", "classes");
        assertTrue(Files.isDirectory(classes), "target/classes 缺失——先 mvn compile");
        try (Stream<Path> files = Files.walk(classes)) {
            // 字节码常量池里的包引用形如 io/debezium/connector/postgresql(斜杠形态),
            // 文本资源中若现该形态同样命中;自有包前缀是 org/vastdata/debezium,不会误命中
            long violations = files.filter(Files::isRegularFile)
                    .filter(p -> containsVanillaReference(p))
                    .count();
            assertTrue(violations == 0, "发现引用 vanilla PG 连接器包的文件: " + violations + " 个");
        }
    }

    private boolean containsVanillaReference(Path file) {
        try {
            byte[] bytes = Files.readAllBytes(file);
            byte[] needle = "io/debezium/connector/postgresql".getBytes();
            outer:
            for (int i = 0; i <= bytes.length - needle.length; i++) {
                for (int j = 0; j < needle.length; j++) {
                    if (bytes[i + j] != needle[j]) {
                        continue outer;
                    }
                }
                return true;
            }
            return false;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
