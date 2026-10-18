import org.vastdata.vbstream.walsource.api.WalSource;
import org.vastdata.vbstream.walsource.changes.ChangeOutputListener;
import org.vastdata.vbstream.walsource.layout.Lsn;

import java.util.Properties;

/**
 * B 档护栏 + catalog-only 面 spike（throwaway）：①DML listener 在场（vb.wal.dml 缺省
 * true）——期望启动期 ISE（兼容模式白名单 {'PG','A'} 外的 'B' 档，M4 指引文案）；
 * ②无 listener（dmlEnabled=false 等价 catalog-only）——期望门面完整装配并在 B 档库上
 * 正常消费 WAL（catalog 面与兼容模式无关的 M1 先例在 VastBase B 档的复验），跑 ~8s
 * 验证 consumedLsn 推进后 close。
 */
public final class BModeFacadeSpike {

    public static void main(String[] args) throws Exception {
        // ① DML 面在场：期望 ISE
        try {
            new WalSource(base(), new SilentListener()).start();
            System.out.println("!! 预期 ISE 未发生——护栏放行了 B 档");
        } catch (IllegalStateException e) {
            System.out.println("== [1] DML 在场 ISE 如期: " + String.valueOf(e.getMessage()).replace('\n', ' '));
        }
        // ② catalog-only：期望装配成功 + 消费推进
        WalSource src = new WalSource(base());
        src.start();
        long start = src.consumedLsn();
        System.out.println("== [2] catalog-only 启动成功: consumedLsn=" + Lsn.format(start)
                + " resumedFromState=" + src.resumedFromState());
        Thread.sleep(8000);
        System.out.println("== [2] 8s 后: consumedLsn=" + Lsn.format(src.consumedLsn())
                + " records=" + src.metrics().records.sum());
        src.close();
        System.out.println("== [2] close 完成");
    }

    private static Properties base() {
        Properties p = new Properties();
        p.setProperty("vb.wal.host", "172.16.107.133");
        p.setProperty("vb.wal.port", "55443");
        p.setProperty("vb.wal.db", "test_b");
        p.setProperty("vb.wal.user", "test");
        p.setProperty("vb.wal.pass", "Tester123");
        p.setProperty("vb.wal.slot", "vb_wal_spike_b_slot");
        p.setProperty("vb.wal.og.source", "direct");
        return p;
    }

    private static final class SilentListener implements ChangeOutputListener {
        @Override public void onBegin(BatchBegin begin) { }
        @Override public void onRow(RowChange row) { }
        @Override public void onEnd(BatchEnd end) { }
        @Override public void onAborted(BatchAborted aborted) { }
    }
}
