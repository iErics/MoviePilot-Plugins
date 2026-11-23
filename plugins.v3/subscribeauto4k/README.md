# 订阅自适应4K (SubscribeAuto4K v2.0.0)

面向 **MoviePilot V3** 重写升级的自适应 4K 订阅收紧与旧集洗版插件。

## 核心机制

1. **三层 4K 信号发现（省流量、不刷站点）**
   - **实时事件（零延迟、零请求）**：监听 `DownloadAdded` / `TransferComplete`，只要当前订阅刚下载或入库 4K，立刻收紧规则组。
   - **本地检测（零外网请求）**：优先扫描整理历史（`TransferHistoryOper`）和本地 RSS/爬虫种子缓存（`TorrentsChain.get_subscribe_cache_candidates`）。
   - **受控主动探查（可开关）**：对超出 `check_ttl_hours` 冷却期的订阅，按 `batch_limit` 公平轮询并调用 `SearchChain().process` 带上原规则组 + `strict_group` 校验可用 4K 资源。
2. **安全收紧与防误伤**
   - 在原 `filter_groups` 末尾**追加** `strict_group`（如 `仅4K`），保留 `免费`、`前置过滤` 与分类规则组。
   - 执行前校验 `strict_group` 是否真实存在；支持 `exclude_groups`、`exclude_keywords` 与 `dry_run` 试运行。
   - 首次修改订阅前自动保存原始 `filter_groups / best_version / note / episode_priority` 快照，支持按 `subscribe_id` 一键恢复。
3. **精准旧集洗版（双模式）**
   - 结合整理历史中的实际文件名分辨率与 `subscribe.episode_priority < upgrade_priority` 精确定位非 4K 旧集，并记录已洗版集号防止死循环。
   - `requeue`（默认推荐）：将确认非 4K 的旧集移出 `note` 并更新 `lack_episode`，下次订阅搜索直接按 `仅4K` 补齐旧集。
   - `best_version`：开启按集洗版并降低旧集优先级，由原生洗版引擎升级。

## 部署到远端 NAS（两种方式）

### 方式 A：作为自定义插件市场仓库（推荐，便于后续持续升级）
1. 将 `plugin_repo/` 目录内容推送到你自己的 GitHub 仓库（`main` 分支，根目录含 `package.v3.json` 和 `plugins.v3/subscribeauto4k/`）；
2. 在 MoviePilot 设置中的 `PLUGIN_MARKET` 添加该仓库地址；
3. 在插件市场安装或更新 `SubscribeAuto4K`，随后执行重载即可。

### 方式 B：直接替换 NAS 本地插件目录
将 `plugins.v3/subscribeauto4k/__init__.py` 覆盖到 NAS MoviePilot 容器的本地插件目录（如 `/app/app/plugins/subscribeauto4k/__init__.py`），然后在 MoviePilot（或通过 `mp call moviepilot_api --op plugin.reload --arg 'path_params={"plugin_id":"SubscribeAuto4K"}'`）热重载插件即可。旧版 v1.0.0 配置字段全部向后兼容。
