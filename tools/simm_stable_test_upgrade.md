# simm_stable_test 升级说明

## 背景

原始版本的 `tools/simm_stable_test.cc` 更偏功能冒烟工具，存在几个明显短板：

- `async` 路径基本未完成，`iodepth` 实际不生效
- 工作负载模型过于单一，难以支撑长期稳定性验证
- 缺少周期性统计与延迟指标，不利于长时间运行时观测
- key 基本一次性生成，无法形成更真实的覆盖写、热点和存活数据压力
- 停止逻辑较粗，长跑时不够稳妥

这次改动的目标，是把它提升成更适合持续压测、稳定性回归和故障观测的工具。

## 主要改动

### 1. 补全 async 模式

- 实现真正的异步提交与回调处理
- `iodepth` 现在实际控制每个 worker 的并发异步请求数
- 支持异步 `put/get/exists/delete`
- `async + batch` 组合当前显式禁止，避免半成品路径误用

### 2. 引入长期稳定 workload 模型

- 每个 worker 持有固定的逻辑 keyspace
- key 在 keyspace 内反复复用，而不是持续只写新 key
- 支持更贴近真实服务的混合流量：
  - `putratio`
  - `getratio`
  - `existsratio`
  - `delratio`
  - `oputratio`

这样可以覆盖：

- 覆盖写
- 老数据读取
- 数据删除后再次访问
- 热点 key 反复更新

### 3. 改进数据校验方式

- 不再为每个 key 保存整份 value 副本
- 改为记录 `(exists, size, seed)` 元信息
- value 内容通过 `seed` 按确定性规则生成和校验

收益：

- 降低工具自身内存开销
- 适合更长时间运行
- 仍可对 `get` 返回内容做一致性校验

### 4. 增加延迟与吞吐统计

新增统计项：

- 总操作数
- 成功/失败计数
- submit 失败计数
- 数据匹配/不匹配计数
- put/get 字节量
- 延迟统计：
  - avg
  - p50
  - p95
  - p99
  - max

### 5. 增加周期性报告

新增 `report_interval_inSecs`：

- 周期性打印增量统计
- 测试结束时打印最终汇总

这让工具更适合：

- 长跑观测
- 问题定位
- 性能回归对比

### 6. 改进退出与收尾

- 注册 `SIGINT` / `SIGTERM`
- 支持收到停止信号后优雅退出
- async worker 在停止时会等待 inflight 请求完成再退出

这能减少：

- 工具自身异常中止
- 长跑结束时统计不完整
- 回调尚未收敛时直接退出的问题

### 7. 改善可用性

- 补充更完整的 flags
- `--help` 现在有正式 usage 信息
- 启动时打印关键测试参数

## 新增/调整的重要参数

- `putratio`
- `getratio`
- `existsratio`
- `delratio`
- `oputratio`
- `keyspace_per_thread`
- `report_interval_inSecs`
- `strict_verify_exists`

## 当前限制

- `async + batch_mode` 暂不支持
- 工具仍以 client 视角做稳定性测试，不替代 server 端 profiling/资源分析
- 延迟统计当前采用分桶估算 percentile，不是全量样本精确分位数

## 构建验证

已完成以下验证：

- `build/release` 下 `simm_stable_test` 构建通过
- `build/debug` 下 `simm_stable_test` 构建通过
- `--help` 启动 smoke test 通过

测试前统一使用：

```bash
export SICL_LOG_LEVEL=WARN
```

## 建议的后续增强

如果后面继续迭代，建议优先考虑：

- 增加错误码分桶统计
- 增加更明确的热点分布模型
- 增加阶段性 summary 文件输出
- 支持 fault injection 联动场景
- 增加更细粒度的 async 超时/回调异常观测

