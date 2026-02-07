import datetime
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from app.chain.download import DownloadChain
from app.chain.search import SearchChain
from app.chain.torrents import TorrentsChain
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfo
from app.helper.mediaserver import MediaServerHelper
from app.helper.torrent import TorrentHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import NotExistMediaInfo
from app.schemas.types import MediaType

_lock = Lock()


class EmbyWatchAccelerator(_PluginBase):
    # 插件名称
    plugin_name = "Emby继续观看加速"
    # 插件描述
    plugin_desc = "基于Emby继续观看的剧集记录，自动搜索并加速更新或补全缺失剧集。"
    # 插件图标
    plugin_icon = "download.png"
    # 插件版本
    plugin_version = "1.0.6"
    # 插件作者
    plugin_author = "codex"
    # 作者主页
    author_url = ""
    # 插件配置项ID前缀
    plugin_config_prefix = "emby_watch_acc_"
    # 加载顺序
    plugin_order = 45
    # 可使用的用户级别
    auth_level = 2

    # 配置项
    _enabled: bool = False
    _accelerate_interval_minutes: int = 10
    _backfill_interval_hours: int = 6
    _resume_limit: int = 50
    _resume_days: int = 30
    _user_whitelist: str = ""
    _user_blacklist: str = ""
    _library_blacklist: str = ""
    _backfill_stats_only: bool = False
    _max_log_records: int = 200
    _run_once: bool = False

    def init_plugin(self, config: dict = None):
        # 停止现有任务
        self.stop_service()

        if config:
            self._enabled = bool(config.get("enabled"))
            self._accelerate_interval_minutes = int(config.get("accelerate_interval_minutes") or 10)
            self._backfill_interval_hours = int(config.get("backfill_interval_hours") or 6)
            self._resume_limit = int(config.get("resume_limit") or 50)
            self._resume_days = int(config.get("resume_days") or 30)
            self._user_whitelist = (config.get("user_whitelist") or "").strip()
            self._user_blacklist = (config.get("user_blacklist") or "").strip()
            self._library_blacklist = (config.get("library_blacklist") or "").strip()
            self._backfill_stats_only = bool(config.get("backfill_stats_only"))
            self._run_once = bool(config.get("run_once"))

        if self._run_once:
            if self._enabled:
                self._append_log("检测到立即运行开关，开始执行一次加速任务")
                self._process(mode="accelerate")
            else:
                self._append_log("立即运行开关已开启，但插件未启用，跳过执行", "WARNING")
            self._run_once = False
            self.update_config({
                "enabled": self._enabled,
                "accelerate_interval_minutes": self._accelerate_interval_minutes,
                "backfill_interval_hours": self._backfill_interval_hours,
                "resume_limit": self._resume_limit,
                "resume_days": self._resume_days,
                "user_whitelist": self._user_whitelist,
                "user_blacklist": self._user_blacklist,
                "library_blacklist": self._library_blacklist,
                "backfill_stats_only": self._backfill_stats_only,
                "run_once": False
            })

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return []

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []
        return [
            {
                "id": "EmbyWatchAccelerate",
                "name": "Emby继续观看加速（更新）",
                "trigger": "interval",
                "func": self._run_accelerate,
                "kwargs": {"minutes": max(self._accelerate_interval_minutes, 1)}
            },
            {
                "id": "EmbyWatchBackfill",
                "name": "Emby继续观看加速（补全）",
                "trigger": "interval",
                "func": self._run_backfill,
                "kwargs": {"hours": max(self._backfill_interval_hours, 1)}
            }
        ]

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "outlined", "class": "pa-3 mb-3"},
                        "content": [
                            {"component": "VCardTitle", "text": "执行开关"},
                            {"component": "VDivider", "props": {"class": "mb-2"}},
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [{"component": "VSwitch", "props": {"model": "enabled", "label": "启用插件"}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [{"component": "VSwitch", "props": {"model": "run_once", "label": "保存后立即运行一次"}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [{"component": "VSwitch", "props": {"model": "backfill_stats_only", "label": "仅统计缺失，不执行补全"}}]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VCard",
                        "props": {"variant": "outlined", "class": "pa-3 mb-3"},
                        "content": [
                            {"component": "VCardTitle", "text": "策略参数"},
                            {"component": "VDivider", "props": {"class": "mb-2"}},
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "accelerate_interval_minutes", "label": "加速更新间隔（分钟）", "type": "number", "min": 1}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "backfill_interval_hours", "label": "补全缺失间隔（小时）", "type": "number", "min": 1}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "resume_limit", "label": "继续观看读取数量", "type": "number", "min": 1}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "resume_days", "label": "最近观看天数范围", "type": "number", "min": 0}}]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VCard",
                        "props": {"variant": "outlined", "class": "pa-3 mb-3"},
                        "content": [
                            {"component": "VCardTitle", "text": "用户过滤"},
                            {"component": "VDivider", "props": {"class": "mb-2"}},
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [{"component": "VTextField", "props": {"model": "user_whitelist", "label": "用户白名单（逗号分隔）"}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [{"component": "VTextField", "props": {"model": "user_blacklist", "label": "用户黑名单（逗号分隔）"}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 12},
                                        "content": [{"component": "VTextarea", "props": {"model": "library_blacklist", "label": "媒体库黑名单（每行：服务器名称:媒体库名称或ID）", "rows": 3, "autoGrow": True}}]
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "info",
                            "variant": "tonal",
                            "text": "仅处理Emby继续观看中的电视剧。开启“仅统计缺失”后，只记录缺失季集，不会执行补全下载。"
                        }
                    }
                ]
            }
        ], {
            "enabled": False,
            "accelerate_interval_minutes": 10,
            "backfill_interval_hours": 6,
            "resume_limit": 50,
            "resume_days": 30,
            "user_whitelist": "",
            "user_blacklist": "",
            "library_blacklist": "",
            "backfill_stats_only": False,
            "run_once": False
        }

    def get_page(self) -> Optional[List[dict]]:
        stats = self.get_data("last_stats") or {}
        logs = self.get_data("logs") or []
        items = [
            {"label": "上次运行时间", "value": stats.get("finished_at") or "-"},
            {"label": "上次运行模式", "value": stats.get("mode") or "-"},
            {"label": "耗时(秒)", "value": stats.get("duration_seconds") or "-"},
            {"label": "服务器数", "value": stats.get("servers") or 0},
            {"label": "继续观看条目", "value": stats.get("resume_items") or 0},
            {"label": "去重剧集数", "value": stats.get("series_items") or 0},
            {"label": "处理剧集数", "value": stats.get("processed_series") or 0},
            {"label": "加速尝试/下载", "value": f"{stats.get('accelerate_attempts') or 0}/{stats.get('accelerate_downloads') or 0}"},
            {"label": "补全尝试/下载", "value": f"{stats.get('backfill_attempts') or 0}/{stats.get('backfill_downloads') or 0}"},
            {"label": "仅统计跳过补全", "value": stats.get("backfill_skipped_stats_only") or 0},
            {"label": "媒体库黑名单跳过", "value": stats.get("skipped_library_blacklist") or 0},
            {"label": "跳过非电视剧", "value": stats.get("skipped_non_tv") or 0},
            {"label": "跳过识别失败", "value": stats.get("skipped_no_mediainfo") or 0},
            {"label": "跳过详情失败", "value": stats.get("skipped_no_seriesinfo") or 0}
        ]
        return [
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "class": "pa-3"
                },
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "text-subtitle-1"},
                        "text": "运行统计"
                    },
                    {
                        "component": "VDivider"
                    },
                    {
                        "component": "VList",
                        "props": {"density": "compact"},
                        "content": [
                            {
                                "component": "VListItem",
                                "props": {"class": "py-1"},
                                "content": [
                                    {"component": "VListItemTitle", "text": f"{item['label']}：{item['value']}"}
                                ]
                            } for item in items
                        ]
                    }
                ]
            },
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "class": "pa-3 mt-3"
                },
                "content": [
                    {
                        "component": "VCardTitle",
                        "props": {"class": "text-subtitle-1"},
                        "text": "最近日志"
                    },
                    {
                        "component": "VDivider"
                    },
                    {
                        "component": "VList",
                        "props": {"density": "compact"},
                        "content": [
                            {
                                "component": "VListItem",
                                "props": {"class": "py-1"},
                                "content": [
                                    {"component": "VListItemTitle", "text": item}
                                ]
                            } for item in (logs[-50:] if logs else ["暂无日志"])
                        ]
                    }
                ]
            }
        ]

    def stop_service(self):
        pass

    def _run_accelerate(self):
        self._process(mode="accelerate")

    def _run_backfill(self):
        self._process(mode="backfill")

    def _append_log(self, message: str, level: str = "INFO") -> None:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{now}] [{level}] {message}"
        logs = self.get_data("logs") or []
        logs.append(line)
        if len(logs) > self._max_log_records:
            logs = logs[-self._max_log_records:]
        self.save_data("logs", logs)

    def _process(self, mode: str):
        if not self._enabled:
            return
        if not _lock.acquire(blocking=False):
            logger.info("继续观看加速任务正在运行，跳过本次执行")
            self._append_log("继续观看加速任务正在运行，跳过本次执行")
            return
        start_time = datetime.datetime.now()
        stats = {
            "servers": 0,
            "resume_items": 0,
            "series_items": 0,
            "processed_series": 0,
            "accelerate_attempts": 0,
            "accelerate_downloads": 0,
            "backfill_attempts": 0,
            "backfill_downloads": 0,
            "backfill_skipped_stats_only": 0,
            "skipped_library_blacklist": 0,
            "skipped_non_tv": 0,
            "skipped_no_mediainfo": 0,
            "skipped_no_seriesinfo": 0
        }
        try:
            logger.info(f"继续观看加速任务开始，模式：{mode}")
            self._append_log(f"继续观看加速任务开始，模式：{mode}")
            services = MediaServerHelper().get_services()
            if not services:
                logger.info("未检测到媒体服务器配置，任务结束")
                self._append_log("未检测到媒体服务器配置，任务结束")
                return
            for name, service in services.items():
                if not service or service.type != "emby" or not service.instance:
                    continue
                logger.info(f"开始处理Emby服务器：{name}")
                self._append_log(f"开始处理Emby服务器：{name}")
                stats["servers"] += 1
                self._process_emby_service(service.instance, mode=mode, stats=stats, server_name=name)
        finally:
            cost = (datetime.datetime.now() - start_time).total_seconds()
            stats["mode"] = mode
            stats["duration_seconds"] = round(cost, 2)
            stats["finished_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_data("last_stats", stats)
            logger.info(
                f"继续观看加速任务结束，模式：{mode}，耗时：{cost:.2f}秒，"
                f"服务器数：{stats['servers']}，继续观看条目：{stats['resume_items']}，"
                f"去重剧集数：{stats['series_items']}，处理剧集数：{stats['processed_series']}，"
                f"加速尝试/下载：{stats['accelerate_attempts']}/{stats['accelerate_downloads']}，"
                f"补全尝试/下载：{stats['backfill_attempts']}/{stats['backfill_downloads']}，"
                f"仅统计跳过补全：{stats['backfill_skipped_stats_only']}，"
                f"媒体库黑名单跳过：{stats['skipped_library_blacklist']}，"
                f"跳过非电视剧：{stats['skipped_non_tv']}，"
                f"跳过识别失败：{stats['skipped_no_mediainfo']}，"
                f"跳过详情失败：{stats['skipped_no_seriesinfo']}"
            )
            self._append_log(
                f"任务结束，模式：{mode}，耗时：{cost:.2f}秒，"
                f"加速尝试/下载：{stats['accelerate_attempts']}/{stats['accelerate_downloads']}，"
                f"补全尝试/下载：{stats['backfill_attempts']}/{stats['backfill_downloads']}，"
                f"仅统计跳过补全：{stats['backfill_skipped_stats_only']}，"
                f"媒体库黑名单跳过：{stats['skipped_library_blacklist']}"
            )
            _lock.release()

    def _process_emby_service(self, emby, mode: str, stats: Dict[str, int], server_name: str = ""):
        resume_items = self._get_resume_items(emby, stats, server_name)
        if not resume_items:
            logger.info("未获取到继续观看的剧集记录")
            return
        stats["resume_items"] += len(resume_items)
        series_items = self._merge_resume_series(resume_items)
        if not series_items:
            logger.info("继续观看记录过滤后为空")
            return
        stats["series_items"] += len(series_items)
        logger.info(f"继续观看剧集数：{len(series_items)}")

        download_chain = DownloadChain()
        search_chain = SearchChain()

        for item in series_items:
            series_id = item.get("series_id")
            current_season = item.get("season")
            if not series_id or not current_season:
                continue

            series_info = emby.get_iteminfo(series_id)
            if not series_info:
                logger.info(f"获取剧集详情失败：{series_id}")
                stats["skipped_no_seriesinfo"] += 1
                continue

            mediainfo = self._get_mediainfo(series_info)
            if not mediainfo:
                logger.info(f"媒体识别失败：{series_info.title}")
                stats["skipped_no_mediainfo"] += 1
                continue

            if mediainfo.type != MediaType.TV:
                logger.info(f"跳过非电视剧：{mediainfo.title_year}")
                stats["skipped_non_tv"] += 1
                continue

            stats["processed_series"] += 1
            meta = MetaInfo(mediainfo.title)
            meta.year = mediainfo.year
            meta.type = MediaType.TV
            meta.begin_season = current_season

            _, no_exists = download_chain.get_no_exists_info(meta=meta, mediainfo=mediainfo)
            emby_status = self._get_emby_series_status(emby, series_id)
            is_ended = self._is_ended(mediainfo, emby_status)
            status_text = "已完结" if is_ended else "更新中"

            if is_ended:
                if no_exists:
                    logger.info(f"{mediainfo.title_year} 状态：{status_text}，执行补全")
                    stats["backfill_attempts"] += 1
                    if self._backfill_stats_only:
                        logger.info(f"{mediainfo.title_year} 仅统计缺失模式，跳过补全下载")
                        stats["backfill_skipped_stats_only"] += 1
                    elif self._backfill_series(search_chain, download_chain, mediainfo, meta, no_exists):
                        stats["backfill_downloads"] += 1
                continue

            if no_exists:
                logger.info(f"{mediainfo.title_year} 状态：{status_text}，存在缺失集，执行补全")
                logger.info(f"{mediainfo.title_year} 缺失详情：{self._format_no_exists(no_exists)}")
                stats["backfill_attempts"] += 1
                if self._backfill_stats_only:
                    logger.info(f"{mediainfo.title_year} 仅统计缺失模式，跳过补全下载")
                    stats["backfill_skipped_stats_only"] += 1
                elif self._backfill_series(search_chain, download_chain, mediainfo, meta, no_exists):
                    stats["backfill_downloads"] += 1
                continue

            if mode == "accelerate":
                logger.info(f"{mediainfo.title_year} 状态：{status_text}，执行加速更新（缓存匹配）")
                stats["accelerate_attempts"] += 1
                if self._accelerate_series(search_chain, download_chain, mediainfo, meta, current_season):
                    stats["accelerate_downloads"] += 1

    def _get_resume_items(self, emby, stats: Optional[Dict[str, int]] = None, server_name: str = "") -> List[dict]:
        users = self._get_emby_users(emby)
        if not users:
            logger.info("用户列表为空，无法获取继续观看")
            return []
        blacklist_names, blacklist_paths = self._build_library_blacklist_for_server(emby, server_name)
        if blacklist_names:
            logger.info(f"媒体库黑名单：{', '.join(sorted(blacklist_names))}")
        all_items: List[dict] = []
        limit = max(self._resume_limit, 1)
        per_user_limit = max(1, int((limit + len(users) - 1) / len(users)))
        logger.info(f"继续观看读取总数上限：{limit}，用户数：{len(users)}，单用户上限：{per_user_limit}")
        for user in users:
            user_id = user.get("Id")
            if not user_id:
                continue
            url = (f"[HOST]emby/Users/{user_id}/Items/Resume"
                   "?Limit=100&MediaTypes=Video"
                   "&Fields=ProviderIds,SeriesId,ParentIndexNumber,IndexNumber,ProductionYear,Path"
                   "&api_key=[APIKEY]")
            res = emby.get_data(url)
            if not res or res.status_code != 200:
                code = res.status_code if res else "no-response"
                logger.error(f"获取Emby继续观看数据失败：{user.get('Name') or user_id}，status={code}")
                continue
            items = res.json().get("Items") or []
            episode_items = [item for item in items if item.get("Type") == "Episode"]
            if blacklist_paths:
                filtered_items = []
                for episode_item in episode_items:
                    if self._is_blacklisted_library_item(episode_item, blacklist_paths):
                        logger.info(f"媒体库黑名单过滤：{self._resume_item_desc(episode_item)}")
                        if stats is not None:
                            stats["skipped_library_blacklist"] = stats.get("skipped_library_blacklist", 0) + 1
                        continue
                    filtered_items.append(episode_item)
                episode_items = filtered_items
            logger.info(f"用户 {user.get('Name') or user_id} 继续观看剧集数：{len(episode_items)}")
            for episode_item in episode_items[:per_user_limit]:
                logger.info(f"继续观看候选：{self._resume_item_desc(episode_item)}")
            all_items.extend(episode_items[:per_user_limit])
            if len(all_items) >= limit:
                break
        return all_items[:limit]

    def _get_emby_users(self, emby) -> List[dict]:
        res = emby.get_data("[HOST]Users?api_key=[APIKEY]")
        if not res or res.status_code != 200:
            logger.error("获取Emby用户列表失败")
            return []
        users = res.json() or []
        whitelist = [u.strip() for u in self._user_whitelist.split(",") if u.strip()]
        blacklist = [u.strip() for u in self._user_blacklist.split(",") if u.strip()]
        if whitelist:
            users = [u for u in users if u.get("Name") in whitelist]
        if blacklist:
            users = [u for u in users if u.get("Name") not in blacklist]
        logger.info(f"Emby用户过滤后数量：{len(users)}")
        return users

    def _build_library_blacklist_for_server(self, emby, server_name: str) -> Tuple[set, List[str]]:
        raw = self._library_blacklist or ""
        rules: List[str] = []
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if ":" not in stripped:
                logger.warning(f"媒体库黑名单规则格式无效（需为 服务器名称:媒体库名称或ID）：{stripped}")
                continue
            rule_server, rule_library = stripped.split(":", 1)
            if rule_server.strip().lower() != (server_name or "").strip().lower():
                continue
            token = rule_library.strip().lower()
            if token:
                rules.append(token)
        if not rules:
            return set(), []
        token_set = set(rules)
        library_paths: List[str] = []
        matched_names: set = set()
        try:
            libraries = emby.get_emby_virtual_folders() or []
        except Exception:
            libraries = []
        for lib in libraries:
            lib_id = str(lib.get("Id") or "").strip().lower()
            lib_name = str(lib.get("Name") or "").strip().lower()
            if lib_id in token_set or lib_name in token_set:
                if lib.get("Name"):
                    matched_names.add(str(lib.get("Name")))
                for path in (lib.get("Path") or []):
                    normalized = str(path).replace("\\", "/").lower().rstrip("/")
                    if normalized:
                        library_paths.append(normalized)
        return matched_names, library_paths

    @staticmethod
    def _is_blacklisted_library_item(item: Dict[str, Any], blacklisted_paths: List[str]) -> bool:
        item_path = str(item.get("Path") or "").replace("\\", "/").lower().rstrip("/")
        if not item_path:
            return False
        for base in blacklisted_paths:
            if item_path.startswith(base):
                return True
        return False

    def _merge_resume_series(self, items: List[dict]) -> List[dict]:
        series_map: Dict[str, dict] = {}
        reason_counter = {
            "missing_series_or_season": 0,
            "invalid_last_played": 0,
            "out_of_resume_days": 0,
            "missing_last_played_kept": 0,
            "duplicate_older": 0
        }
        now = datetime.datetime.now()
        for item in items:
            series_id = item.get("SeriesId")
            season = item.get("ParentIndexNumber")
            episode = item.get("IndexNumber")
            item_desc = self._resume_item_desc(item)
            if not series_id or not season:
                reason_counter["missing_series_or_season"] += 1
                logger.info(f"排除继续观看条目：缺少SeriesId或Season，{item_desc}")
                continue
            key = f"{series_id}:{season}"
            last_played = item.get("UserData", {}).get("LastPlayedDate")
            last_played_dt = self._parse_last_played(last_played)
            if self._resume_days and last_played_dt:
                if (now - last_played_dt).days > self._resume_days:
                    reason_counter["out_of_resume_days"] += 1
                    logger.info(
                        f"排除继续观看条目：超出天数范围，{item_desc}，"
                        f"last_played={last_played_dt.strftime('%Y-%m-%d %H:%M:%S')}"
                    )
                    continue
            elif self._resume_days and not last_played_dt:
                if last_played:
                    reason_counter["invalid_last_played"] += 1
                    logger.info(
                        f"继续保留条目：LastPlayedDate无法解析，{item_desc}，raw={last_played}"
                    )
                else:
                    reason_counter["missing_last_played_kept"] += 1
                    logger.info(f"继续保留条目：缺少LastPlayedDate，{item_desc}")
            record = series_map.get(key)
            if not record or (last_played_dt and last_played_dt > record.get("last_played", datetime.datetime.min)):
                series_map[key] = {
                    "series_id": series_id,
                    "season": int(season) if season else None,
                    "episode": int(episode) if episode else None,
                    "last_played": last_played_dt or datetime.datetime.min
                }
            else:
                reason_counter["duplicate_older"] += 1
                logger.info(f"排除继续观看条目：同剧同季重复且较旧，{item_desc}")
        logger.info(
            "继续观看过滤统计："
            f"缺少series/season={reason_counter['missing_series_or_season']}，"
            f"时间解析失败(保留)={reason_counter['invalid_last_played']}，"
            f"超出天数={reason_counter['out_of_resume_days']}，"
            f"缺少时间(保留)={reason_counter['missing_last_played_kept']}，"
            f"重复较旧={reason_counter['duplicate_older']}"
        )
        logger.info(f"继续观看去重后剧集数：{len(series_map)}")
        return list(series_map.values())

    @staticmethod
    def _parse_last_played(last_played: Optional[str]) -> Optional[datetime.datetime]:
        if not last_played:
            return None
        raw = str(last_played).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        try:
            parsed = datetime.datetime.fromisoformat(raw)
            if parsed.tzinfo:
                return parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return parsed
        except Exception:
            try:
                if "." in raw:
                    raw = raw.split(".")[0]
                return datetime.datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S")
            except Exception:
                return None

    @staticmethod
    def _resume_item_desc(item: Dict[str, Any]) -> str:
        series_name = item.get("SeriesName") or item.get("Name") or "未知剧集"
        episode_name = item.get("Name") or "-"
        season = item.get("ParentIndexNumber")
        episode = item.get("IndexNumber")
        item_id = item.get("Id")
        return f"title={series_name}，episode={episode_name}，S{season}E{episode}，item_id={item_id}"

    def _get_mediainfo(self, series_info) -> Optional[MediaInfo]:
        mediainfo = None
        if series_info.tmdbid:
            mediainfo = self.chain.recognize_media(mtype=MediaType.TV, tmdbid=series_info.tmdbid)
        if not mediainfo:
            meta = MetaInfo(series_info.title)
            meta.year = series_info.year
            meta.type = MediaType.TV
            mediainfo = self.chain.recognize_media(meta=meta, mtype=MediaType.TV)
        return mediainfo

    @staticmethod
    def _is_ended(mediainfo: MediaInfo, emby_status: Optional[str] = None) -> bool:
        ended_status = {"ended", "canceled", "cancelled", "完结", "已完结"}
        active_status = {"continuing", "returning series", "upcoming", "更新中", "连载中"}
        if emby_status:
            normalized = emby_status.strip().lower()
            if normalized in ended_status:
                return True
            if normalized in active_status:
                return False
        status = (mediainfo.status or "").strip().lower()
        return status in ended_status

    def _accelerate_series(self, search_chain: SearchChain, download_chain: DownloadChain,
                           mediainfo: MediaInfo, meta: MetaInfo, season: int) -> bool:
        mediakey = mediainfo.tmdb_id or mediainfo.douban_id
        if not mediakey:
            return False
        contexts = self._accelerate_contexts_from_cache(mediainfo=mediainfo, season=season)
        logger.info(f"{mediainfo.title_year} 加速缓存命中资源数：{len(contexts)}")
        if not contexts:
            logger.info(f"{mediainfo.title_year} 加速缓存未命中，跳过本次加速（不触发全站搜索）")
            return False

        exist_info = self.chain.media_exists(mediainfo=mediainfo)
        existing_episodes = set()
        if exist_info and exist_info.seasons:
            existing_episodes = set(exist_info.seasons.get(season) or [])

        for context in contexts:
            meta = context.meta_info
            if not meta.episode_list:
                continue
            if not set(meta.episode_list).difference(existing_episodes):
                continue
            logger.info(f"加速更新下载：{mediainfo.title_year} {meta.season_episode}")
            download_chain.download_single(
                context=context,
                username=self.plugin_name,
                source="EmbyResumeAccelerate"
            )
            return True
        return False

    def _accelerate_contexts_from_cache(self, mediainfo: MediaInfo, season: int) -> List:
        torrents_cache = TorrentsChain().get_torrents() or {}
        contexts = []
        for domain_contexts in torrents_cache.values():
            for context in domain_contexts:
                cache_media = context.media_info
                cache_meta = context.meta_info
                if not cache_media or not cache_meta:
                    continue
                if cache_media.type != MediaType.TV:
                    continue
                same_media = False
                if mediainfo.tmdb_id and cache_media.tmdb_id and mediainfo.tmdb_id == cache_media.tmdb_id:
                    same_media = True
                elif mediainfo.douban_id and cache_media.douban_id and mediainfo.douban_id == cache_media.douban_id:
                    same_media = True
                if not same_media:
                    continue
                season_list = cache_meta.season_list or [1]
                if season not in season_list:
                    continue
                contexts.append(context)
        return TorrentHelper().sort_torrents(contexts)

    @staticmethod
    def _get_emby_series_status(emby, series_id: str) -> Optional[str]:
        if not series_id:
            return None
        url = f"[HOST]emby/Users/[USER]/Items/{series_id}?Fields=Status&api_key=[APIKEY]"
        res = emby.get_data(url)
        if not res or res.status_code != 200:
            return None
        data = res.json() or {}
        return data.get("Status")

    def _backfill_series(self, search_chain: SearchChain, download_chain: DownloadChain,
                         mediainfo: MediaInfo, meta: MetaInfo,
                         no_exists: Dict[int, Dict[int, NotExistMediaInfo]]) -> bool:
        contexts = search_chain.process(mediainfo=mediainfo, no_exists=no_exists)
        logger.info(f"{mediainfo.title_year} 补全搜索命中资源数：{len(contexts)}")
        if not contexts:
            return False
        download_chain.batch_download(
            contexts=contexts,
            no_exists=no_exists,
            username=self.plugin_name,
            source="EmbyResumeBackfill"
        )
        return True

    @staticmethod
    def _format_no_exists(no_exists: Dict[int, Dict[int, NotExistMediaInfo]]) -> str:
        parts = []
        for _, season_map in (no_exists or {}).items():
            for season, info in (season_map or {}).items():
                episodes = info.episodes or []
                if episodes:
                    episodes = sorted(set(episodes))
                    if len(episodes) <= 20:
                        ep_text = ",".join(str(ep) for ep in episodes)
                    else:
                        ep_text = f"{episodes[0]}-{episodes[-1]}(共{len(episodes)}集)"
                else:
                    ep_text = "整季"
                total = f"/总{info.total_episode}集" if info.total_episode else ""
                parts.append(f"S{season}:缺[{ep_text}]{total}")
        return "；".join(parts) if parts else "无"
