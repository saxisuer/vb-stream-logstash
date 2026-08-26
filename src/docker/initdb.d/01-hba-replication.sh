#!/bin/bash
# 首次初始化时执行：在 pg_hba.conf 末尾放行来自容器外（宿主机/其他容器）的
# 逻辑复制连接。initdb 默认只对 localhost 放行 replication，而宿主机经端口
# 映射进来的源地址是 Docker 网关，不追加此行复制连接会被 pg_hba 拒绝。
set -e
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
