package org.vastdata.debezium.connector.postgresql.stream;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

/**
 * 责任:模块自含性边界钉死——扫描 target/classes 全部 .class 字节码,断言无任何
 * io/debezium/connector/postgresql 常量池引用(vanilla 连接器类不得回流)。
 * 边界:仅静态扫描已编译产物,不运行期加载;缺 target/classes(未编译直接跑)时
 * 先由 surefire 的 test-compile 阶段保证存在,找不到即 fail。
 * 线程约束:无共享状态。
 */
class VanillaBoundaryTest {

    @Test
    void noClassReferencesVanillaPostgresConnectorPackage() throws IOException {
        Path classes = Path.of("target", "classes");
        assertTrue(Files.isDirectory(classes), "target/classes 缺失——先 mvn compile");
        try (Stream<Path> files = Files.walk(classes)) {
            // 字节码常量池里的包引用形如 io/debezium/connector/postgresql(斜杠形态);
            // 自有包前缀是 org/vastdata/debezium,不会误命中
            long violations = files.filter(p -> p.toString().endsWith(".class"))
                    .filter(p -> containsVanillaReference(p))
                    .count();
            assertTrue(violations == 0, "发现引用 vanilla PG 连接器包的类: " + violations + " 个");
        }
    }

    private boolean containsVanillaReference(Path classFile) {
        try {
            byte[] bytes = Files.readAllBytes(classFile);
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
