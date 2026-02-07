import datetime
import re
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
    plugin_desc = "基于Emby继续观看的剧集记录，自动搜索并追更更新或补全缺失剧集。"
    # 插件图标
    plugin_icon = "download.png"
    # 插件版本
    plugin_version = "1.0.21"
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
                self._append_log("检测到立即运行开关，开始执行一次追更任务")
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
                "name": "Emby继续观看追更（更新）",
                "trigger": "interval",
                "func": self._run_accelerate,
                "kwargs": {"minutes": max(self._accelerate_interval_minutes, 1)}
            },
            {
                "id": "EmbyWatchBackfill",
                "name": "Emby继续观看追更（补全）",
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
                                        "content": [{"component": "VTextField", "props": {"model": "accelerate_interval_minutes", "label": "追更更新间隔（分钟）", "type": "number", "min": 1}}]
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
                                        "content": [{"component": "VTextarea", "props": {"model": "library_blacklist", "label": "媒体库黑名单（兼容逗号分隔；每行可写 服务器名称:媒体库名称或ID）", "rows": 3, "autoGrow": True}}]
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
        if self._hydrate_stats_posters(stats):
            self.save_data("last_stats", stats)
        user_stats = stats.get("user_stats") or {}
        user_cards = []
        for user_name in sorted(user_stats.keys()):
            user_info = user_stats.get(user_name) or {}
            track_items = user_info.get("track_items") or []
            backfill_items = user_info.get("backfill_items") or []
            # 两个栏目都无数据时，隐藏该用户
            if not track_items and not backfill_items:
                continue

            mode_blocks = []
            if track_items:
                mode_blocks.append(
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "class": "pb-0"},
                        "content": self._build_user_mode_block(
                            title="追更",
                            attempts=0,
                            downloads=0,
                            items=track_items
                        )
                    }
                )
            if backfill_items:
                mode_blocks.append(
                    {
                        "component": "VCol",
                        "props": {"cols": 12, "class": "pt-2"},
                        "content": self._build_user_mode_block(
                            title="补全",
                            attempts=0,
                            downloads=0,
                            items=backfill_items
                        )
                    }
                )
            user_cards.append(
                {
                    "component": "VCard",
                    "props": {"variant": "outlined", "class": "pa-3 mb-3", "style": "border-color:#e6ebf2;"},
                    "content": [
                        {
                            "component": "div",
                            "props": {"class": "d-flex justify-space-between align-center"},
                            "content": [
                                {"component": "VCardTitle", "props": {"class": "pa-0"}, "text": user_name},
                                {
                                    "component": "div",
                                    "props": {"class": "d-flex align-center mr-n2"},
                                    "content": [
                                        {
                                            "component": "VChip",
                                            "props": {"class": "mr-1", "color": "primary", "variant": "tonal", "size": "small"},
                                            "text": f"追更 {user_info.get('track_attempts', 0)}/{user_info.get('track_downloads', 0)}"
                                        },
                                        {
                                            "component": "VChip",
                                            "props": {"color": "success", "variant": "tonal", "size": "small"},
                                            "text": f"补全 {user_info.get('backfill_attempts', 0)}/{user_info.get('backfill_downloads', 0)}"
                                        }
                                    ]
                                }
                            ]
                        },
                        {"component": "VDivider"},
                        {
                            "component": "VRow",
                            "content": mode_blocks
                        }
                    ]
                }
            )
        user_group_content = [
            {"component": "VCardTitle", "props": {"class": "text-subtitle-1"}, "text": "用户分组统计"},
            {"component": "VDivider"}
        ]
        if user_cards:
            user_group_content.extend(user_cards)
        else:
            user_group_content.append({"component": "VCardText", "text": "暂无用户数据"})
        return [
            {
                "component": "VCard",
                "props": {
                    "variant": "outlined",
                    "class": "pa-3",
                    "style": "border-color:#e9edf4;"
                },
                "content": user_group_content
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

    @staticmethod
    def _get_user_bucket(stats: Dict[str, Any], user_name: Optional[str]) -> Dict[str, Any]:
        user_key = user_name or "未知用户"
        user_stats = stats.setdefault("user_stats", {})
        bucket = user_stats.get(user_key)
        if bucket:
            return bucket
        bucket = {
            "resume_items": 0,
            "series_items": 0,
            "processed_series": 0,
            "track_attempts": 0,
            "track_downloads": 0,
            "backfill_attempts": 0,
            "backfill_downloads": 0,
            "track_items": [],
            "backfill_items": []
        }
        user_stats[user_key] = bucket
        return bucket

    def _append_user_media_result(self, stats: Dict[str, Any], user_name: Optional[str], kind: str,
                                  mediainfo: MediaInfo, season: Optional[int], result: str,
                                  series_id: Optional[str] = None, server_name: Optional[str] = None,
                                  emby=None):
        bucket = self._get_user_bucket(stats, user_name)
        target_key = "track_items" if kind == "track" else "backfill_items"
        poster, poster_source = self._resolve_poster_url(mediainfo=mediainfo, emby=emby, series_id=series_id)
        bucket[target_key].append({
            "title": mediainfo.title,
            "year": mediainfo.year,
            "season": season,
            "result": result,
            "poster": poster,
            "poster_source": poster_source,
            "series_id": str(series_id) if series_id else "",
            "server": server_name or "",
            "type": str(getattr(mediainfo.type, "value", mediainfo.type) or "电视剧"),
            "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        logger.info(
            f"统计封面来源：title={mediainfo.title_year}，user={user_name or '-'}，"
            f"source={poster_source}，has_poster={'是' if bool(poster) else '否'}"
        )

    def _resolve_poster_url(self, mediainfo: MediaInfo, emby=None, series_id: Optional[str] = None) -> Tuple[str, str]:
        # 先使用识别链路提供的海报，避免额外网络请求。
        poster = mediainfo.get_poster_image() if mediainfo else None
        if poster:
            return poster, "mediainfo.poster"
        if mediainfo:
            backdrop = mediainfo.get_backdrop_image()
            if backdrop:
                return backdrop, "mediainfo.backdrop"
        if emby and series_id:
            remote = emby.get_remote_image_by_id(series_id, "Primary")
            if remote:
                return remote, "emby.remote_primary"
            local = self._build_emby_image_url(emby=emby, item_id=series_id, image_type="Primary")
            if local:
                return local, "emby.local_primary"
        return "", "none"

    @staticmethod
    def _build_emby_image_url(emby, item_id: Optional[str], image_type: str = "Primary") -> str:
        if not emby or not item_id:
            return ""
        host = str(getattr(emby, "_host", "") or "")
        apikey = str(getattr(emby, "_apikey", "") or "")
        if not host or not apikey:
            return ""
        return f"{host}emby/Items/{item_id}/Images/{image_type}?api_key={apikey}"

    def _hydrate_stats_posters(self, stats: Dict[str, Any]) -> bool:
        services = MediaServerHelper().get_services() or {}
        user_stats = stats.get("user_stats") or {}
        changed = False
        for _, user_info in user_stats.items():
            for key in ("track_items", "backfill_items"):
                items = user_info.get(key) or []
                for item in items:
                    if item.get("poster"):
                        continue
                    series_id = str(item.get("series_id") or "").strip()
                    server_name = str(item.get("server") or "").strip()
                    emby = None
                    if server_name:
                        service = services.get(server_name)
                        if service and service.type == "emby" and service.instance:
                            emby = service.instance
                    if emby and series_id:
                        remote = emby.get_remote_image_by_id(series_id, "Primary")
                        if remote:
                            item["poster"] = remote
                            item["poster_source"] = "emby.remote_primary.hydrate"
                            changed = True
                            continue
                        local = self._build_emby_image_url(emby=emby, item_id=series_id, image_type="Primary")
                        if local:
                            item["poster"] = local
                            item["poster_source"] = "emby.local_primary.hydrate"
                            changed = True
                            continue
                    title = item.get("title")
                    if not title:
                        continue
                    meta = MetaInfo(title)
                    year = item.get("year")
                    if year:
                        meta.year = year
                    meta.type = MediaType.TV
                    try:
                        mediainfo = self.chain.recognize_media(meta=meta, mtype=MediaType.TV)
                    except Exception:
                        mediainfo = None
                    if not mediainfo:
                        continue
                    poster = mediainfo.get_poster_image() or mediainfo.get_backdrop_image()
                    if poster:
                        item["poster"] = poster
                        item["poster_source"] = "mediainfo.hydrate"
                        changed = True
        return changed

    @staticmethod
    def _build_user_mode_block(title: str, attempts: int, downloads: int, items: List[Dict[str, Any]]) -> List[dict]:
        placeholder_poster = (
            "data:image/svg+xml;utf8,"
            "<svg xmlns='http://www.w3.org/2000/svg' width='240' height='360' viewBox='0 0 240 360'>"
            "<rect width='240' height='360' fill='%23eceff1'/>"
            "<text x='120' y='178' text-anchor='middle' fill='%2390a4ae' font-size='20'>No Cover</text>"
            "</svg>"
        )
        cards = []
        for item in (items[-12:] if items else []):
            cards.append({
                "component": "VCol",
                "props": {"cols": 12, "sm": 6, "md": 4, "lg": 3},
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal", "class": "pa-2 h-100", "style": "max-width:360px;width:100%;"},
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "d-flex flex-row flex-nowrap align-start"},
                                "content": [
                                    {
                                        "component": "div",
                                        "props": {"class": "mr-2 flex-shrink-0", "style": "width:72px;height:108px;"},
                                        "content": [
                                            {
                                                "component": "VImg",
                                                "props": {
                                                    "src": item.get("poster") or placeholder_poster,
                                                    "width": "72px",
                                                    "height": "108px",
                                                    "aspect-ratio": "2/3",
                                                    "class": "rounded",
                                                    "cover": True
                                                }
                                            }
                                        ]
                                    },
                                    {
                                        "component": "div",
                                        "content": [
                                            {"component": "VCardSubtitle",
                                             "props": {"class": "pa-0 font-bold"},
                                             "text": item.get("title") or "-"},
                                            {"component": "VCardText",
                                             "props": {"class": "pa-0"},
                                             "text": f"类型：{item.get('type') or '电视剧'}"},
                                            {"component": "VCardText",
                                             "props": {"class": "pa-0"},
                                             "text": f"年份：{item.get('year') or '-'}"},
                                            {"component": "VCardText",
                                             "props": {"class": "pa-0"},
                                             "text": f"季度：第{item.get('season')}季" if item.get("season") else "季度：-"},
                                            {"component": "VCardText",
                                             "props": {"class": "pa-0"},
                                             "text": f"结果：{item.get('result') or '-'}"},
                                            {"component": "VCardText",
                                             "props": {"class": "pa-0 text-no-wrap"},
                                             "text": f"时间：{item.get('time') or '-'}"}
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            })

        content = [
            {
                "component": "div",
                "props": {"class": "position-relative mb-4 mt-1", "style": "height:24px;"},
                "content": [
                    {
                        "component": "VDivider",
                        "props": {
                            "class": "position-absolute",
                            "style": "left:0;right:0;top:50%;transform:translateY(-50%);"
                        }
                    },
                    {
                        "component": "div",
                        "props": {
                            "class": "position-absolute px-3 font-weight-bold text-medium-emphasis",
                            "style": "left:50%;top:50%;transform:translate(-50%,-50%);background:rgba(255,255,255,0.96);"
                        },
                        "text": title
                    }
                ]
            }
        ]
        if cards:
            content.append({"component": "VRow", "content": cards})
        else:
            content.append({"component": "VCardText", "props": {"class": "pa-0 mt-1"}, "text": "暂无记录"})
        return content

    def _process(self, mode: str):
        if not self._enabled:
            return
        if not _lock.acquire(blocking=False):
            logger.info("继续观看任务正在运行，跳过本次执行")
            self._append_log("继续观看任务正在运行，跳过本次执行")
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
            "skipped_no_seriesinfo": 0,
            "user_stats": {}
        }
        try:
            logger.info(f"继续观看任务开始，模式：{mode}")
            self._append_log(f"继续观看任务开始，模式：{mode}")
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
                f"继续观看任务结束，模式：{mode}，耗时：{cost:.2f}秒，"
                f"服务器数：{stats['servers']}，继续观看条目：{stats['resume_items']}，"
                f"去重剧集数：{stats['series_items']}，处理剧集数：{stats['processed_series']}，"
                f"追更尝试/下载：{stats['accelerate_attempts']}/{stats['accelerate_downloads']}，"
                f"补全尝试/下载：{stats['backfill_attempts']}/{stats['backfill_downloads']}，"
                f"仅统计跳过补全：{stats['backfill_skipped_stats_only']}，"
                f"媒体库黑名单跳过：{stats['skipped_library_blacklist']}，"
                f"跳过非电视剧：{stats['skipped_non_tv']}，"
                f"跳过识别失败：{stats['skipped_no_mediainfo']}，"
                f"跳过详情失败：{stats['skipped_no_seriesinfo']}"
            )
            self._append_log(
                f"任务结束，模式：{mode}，耗时：{cost:.2f}秒，"
                f"追更尝试/下载：{stats['accelerate_attempts']}/{stats['accelerate_downloads']}，"
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
            current_user = item.get("user")
            if not series_id or not current_season:
                continue
            user_bucket = self._get_user_bucket(stats, current_user)
            user_bucket["series_items"] += 1

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
            user_bucket["processed_series"] += 1
            meta = MetaInfo(mediainfo.title)
            meta.year = mediainfo.year
            meta.type = MediaType.TV
            meta.begin_season = current_season

            _, no_exists = download_chain.get_no_exists_info(meta=meta, mediainfo=mediainfo)
            actionable_no_exists = self._trim_no_exists_for_current_airing(
                no_exists=no_exists,
                mediainfo=mediainfo,
                current_season=current_season
            )
            is_ended, status_reason = self._resolve_season_state(
                mediainfo=mediainfo,
                current_season=current_season,
                no_exists=actionable_no_exists
            )
            status_text = "已完结" if is_ended else "更新中"
            logger.info(
                f"{mediainfo.title_year} 状态判定：TMDB={mediainfo.status or '-'}，"
                f"next_season={self._next_episode_season(mediainfo) or '-'}，"
                f"next_episode={self._next_episode_number(mediainfo) or '-'}，"
                f"当前季=S{current_season}，缺失={'是' if actionable_no_exists else '否'}，"
                f"结果={status_text}，依据={status_reason}"
            )

            if is_ended:
                if no_exists:
                    logger.info(f"{mediainfo.title_year} 状态：{status_text}，执行补全")
                    stats["backfill_attempts"] += 1
                    user_bucket["backfill_attempts"] += 1
                    if self._backfill_stats_only:
                        logger.info(f"{mediainfo.title_year} 仅统计缺失模式，跳过补全下载")
                        stats["backfill_skipped_stats_only"] += 1
                        self._append_user_media_result(
                            stats=stats, user_name=current_user, kind="backfill",
                            mediainfo=mediainfo, season=current_season, result="仅统计缺失",
                            series_id=series_id, server_name=server_name, emby=emby
                        )
                    elif self._backfill_series(search_chain, download_chain, mediainfo, meta, no_exists):
                        stats["backfill_downloads"] += 1
                        user_bucket["backfill_downloads"] += 1
                        self._append_user_media_result(
                            stats=stats, user_name=current_user, kind="backfill",
                            mediainfo=mediainfo, season=current_season, result="已下载",
                            series_id=series_id, server_name=server_name, emby=emby
                        )
                    else:
                        self._append_user_media_result(
                            stats=stats, user_name=current_user, kind="backfill",
                            mediainfo=mediainfo, season=current_season, result="未命中资源",
                            series_id=series_id, server_name=server_name, emby=emby
                        )
                continue

            if actionable_no_exists:
                logger.info(f"{mediainfo.title_year} 状态：{status_text}，存在缺失集，执行补全")
                logger.info(f"{mediainfo.title_year} 缺失详情：{self._format_no_exists(actionable_no_exists)}")
                stats["backfill_attempts"] += 1
                user_bucket["backfill_attempts"] += 1
                if self._backfill_stats_only:
                    logger.info(f"{mediainfo.title_year} 仅统计缺失模式，跳过补全下载")
                    stats["backfill_skipped_stats_only"] += 1
                    self._append_user_media_result(
                        stats=stats, user_name=current_user, kind="backfill",
                        mediainfo=mediainfo, season=current_season, result="仅统计缺失",
                        series_id=series_id, server_name=server_name, emby=emby
                    )
                elif self._backfill_series(search_chain, download_chain, mediainfo, meta, actionable_no_exists):
                    stats["backfill_downloads"] += 1
                    user_bucket["backfill_downloads"] += 1
                    self._append_user_media_result(
                        stats=stats, user_name=current_user, kind="backfill",
                        mediainfo=mediainfo, season=current_season, result="已下载",
                        series_id=series_id, server_name=server_name, emby=emby
                    )
                else:
                    self._append_user_media_result(
                        stats=stats, user_name=current_user, kind="backfill",
                        mediainfo=mediainfo, season=current_season, result="未命中资源",
                        series_id=series_id, server_name=server_name, emby=emby
                    )
                continue
            if no_exists and not actionable_no_exists:
                logger.info(f"{mediainfo.title_year} 当前缺失均为未播集，跳过补全并进入追更策略")

            if mode == "accelerate":
                logger.info(f"{mediainfo.title_year} 状态：{status_text}，执行追更更新（缓存匹配）")
                stats["accelerate_attempts"] += 1
                user_bucket["track_attempts"] += 1
                if self._accelerate_series(search_chain, download_chain, mediainfo, meta, current_season):
                    stats["accelerate_downloads"] += 1
                    user_bucket["track_downloads"] += 1
                    self._append_user_media_result(
                        stats=stats, user_name=current_user, kind="track",
                        mediainfo=mediainfo, season=current_season, result="已下载",
                        series_id=series_id, server_name=server_name, emby=emby
                    )
                else:
                    self._append_user_media_result(
                        stats=stats, user_name=current_user, kind="track",
                        mediainfo=mediainfo, season=current_season, result="未命中资源",
                        series_id=series_id, server_name=server_name, emby=emby
                    )

    def _get_resume_items(self, emby, stats: Optional[Dict[str, int]] = None, server_name: str = "") -> List[dict]:
        users = self._get_emby_users(emby)
        if not users:
            logger.info("用户列表为空，无法获取继续观看")
            return []
        blacklist_names, blacklist_paths, blacklist_library_ids = self._build_library_blacklist_for_server(emby, server_name)
        if blacklist_names or blacklist_paths or blacklist_library_ids:
            logger.info(
                f"媒体库黑名单已生效：名称={','.join(sorted(blacklist_names)) or '-'}，"
                f"路径数={len(blacklist_paths)}，库ID数={len(blacklist_library_ids)}"
            )
        all_items: List[dict] = []
        resume_schema_logged = False
        limit = max(self._resume_limit, 1)
        per_user_limit = max(1, int((limit + len(users) - 1) / len(users)))
        logger.info(f"继续观看读取总数上限：{limit}，用户数：{len(users)}，单用户上限：{per_user_limit}")
        for user in users:
            user_id = user.get("Id")
            if not user_id:
                continue
            user_name = user.get("Name") or user_id
            url = (f"[HOST]emby/Users/{user_id}/Items/Resume"
                   "?Limit=100&MediaTypes=Video"
                   "&Fields=ProviderIds,SeriesId,ParentIndexNumber,IndexNumber,ProductionYear,Path,AncestorIds"
                   "&api_key=[APIKEY]")
            res = emby.get_data(url)
            if not res or res.status_code != 200:
                code = res.status_code if res else "no-response"
                logger.error(f"获取Emby继续观看数据失败：{user.get('Name') or user_id}，status={code}")
                continue
            items = res.json().get("Items") or []
            episode_items = [item for item in items if item.get("Type") == "Episode"]
            filtered_by_resume = []
            for episode_item in episode_items:
                valid, reason = self._is_valid_resume_item(episode_item)
                tagged_item = dict(episode_item)
                tagged_item["_mp_user"] = user_name
                if valid:
                    if reason == "playback_ticks_zero":
                        logger.info(f"继续保留条目：PlaybackPositionTicks=0，可能为未开始播放的最新集，{self._resume_item_desc(tagged_item)}")
                    filtered_by_resume.append(tagged_item)
                    continue
            episode_items = filtered_by_resume
            if not resume_schema_logged and episode_items:
                self._log_resume_schema_probe(user_name=user_name, episode_items=episode_items)
                resume_schema_logged = True
            if blacklist_paths or blacklist_library_ids or blacklist_names:
                filtered_items = []
                for episode_item in episode_items:
                    if self._is_blacklisted_library_item(
                            episode_item,
                            blacklisted_paths=blacklist_paths,
                            blacklisted_library_ids=blacklist_library_ids,
                            blacklisted_library_names=blacklist_names):
                        logger.info(f"媒体库黑名单过滤：{self._resume_item_desc(episode_item)}")
                        if stats is not None:
                            stats["skipped_library_blacklist"] = stats.get("skipped_library_blacklist", 0) + 1
                        continue
                    filtered_items.append(episode_item)
                episode_items = filtered_items
            logger.info(f"用户 {user_name} 继续观看剧集数：{len(episode_items)}")
            selected_items = episode_items[:per_user_limit]
            self._get_user_bucket(stats, user_name)["resume_items"] += len(selected_items)
            for episode_item in selected_items:
                logger.info(f"继续观看候选：{self._resume_item_desc(episode_item)}")
            all_items.extend(selected_items)
            if len(all_items) >= limit:
                break
        return all_items[:limit]

    @staticmethod
    def _log_resume_schema_probe(user_name: str, episode_items: List[dict]) -> None:
        sample_size = min(3, len(episode_items))
        sample_payload = []
        for item in episode_items[:sample_size]:
            sample_payload.append({
                "Id": item.get("Id"),
                "SeriesId": item.get("SeriesId"),
                "SeriesName": item.get("SeriesName"),
                "ParentId": item.get("ParentId"),
                "ParentIndexNumber": item.get("ParentIndexNumber"),
                "IndexNumber": item.get("IndexNumber"),
                "Path": item.get("Path"),
                "AncestorIds": item.get("AncestorIds"),
                "PlaybackPositionTicks": (item.get("UserData") or {}).get("PlaybackPositionTicks"),
                "LastPlayedDate": (item.get("UserData") or {}).get("LastPlayedDate"),
                "keys": sorted(list(item.keys()))
            })
        logger.info(
            f"Resume字段探针：user={user_name}，样本数={sample_size}，"
            f"样本摘要={sample_payload}"
        )

    @staticmethod
    def _is_valid_resume_item(item: Dict[str, Any]) -> Tuple[bool, str]:
        user_data = item.get("UserData") or {}
        if user_data.get("Played") is True:
            return False, "played_true"
        ticks = user_data.get("PlaybackPositionTicks")
        if ticks is None:
            return True, "ok"
        try:
            if int(ticks) > 0:
                return True, "ok"
            # 继续观看里的 0 进度常见于“最新集刚打开/待开始”，不直接排除
            return True, "playback_ticks_zero"
        except Exception:
            return True, "ok"

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

    def _build_library_blacklist_for_server(self, emby, server_name: str) -> Tuple[set, List[str], set]:
        raw = self._library_blacklist or ""
        rules: List[str] = []
        global_rules: List[str] = []
        current_server = (server_name or "").strip().lower()
        for line in raw.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            segments = [seg.strip() for seg in stripped.replace("，", ",").split(",") if seg.strip()]
            for segment in segments:
                # 允许 server:library 与 server：library 两种写法，且支持同一行多条
                match = re.match(r"^([^:：]+)\s*[:：]\s*(.+)$", segment)
                if not match:
                    global_rules.append(segment.lower())
                    continue
                rule_server = match.group(1).strip().lower()
                rule_library = match.group(2).strip().lower()
                if not rule_library:
                    continue
                if rule_server in {"*", "all", "全部"}:
                    global_rules.append(rule_library)
                    continue
                if rule_server == current_server:
                    rules.append(rule_library)
        all_rules = global_rules + rules
        if not all_rules:
            return set(), [], set()
        token_set = set(all_rules)
        library_paths: List[str] = []
        library_ids: set = set()
        matched_names: set = set()
        configured_name_tokens: set = set(token_set)
        try:
            libraries = emby.get_emby_virtual_folders() or []
        except Exception:
            libraries = []
        if not libraries:
            logger.warning(f"Emby虚拟库列表为空，server={server_name or '-'}")
        for lib in libraries:
            raw_id_keys = [lib.get("Id"), lib.get("ItemId"), lib.get("CollectionFolderId")]
            lib_ids = [str(raw_id).strip().lower() for raw_id in raw_id_keys if raw_id]
            lib_name = str(lib.get("Name") or "").strip().lower()
            if any(lib_id in token_set for lib_id in lib_ids) or lib_name in token_set:
                if lib.get("Name"):
                    matched_name = str(lib.get("Name"))
                    matched_names.add(matched_name)
                    configured_name_tokens.add(matched_name.strip().lower())
                for lib_id in lib_ids:
                    library_ids.add(lib_id)
                raw_paths = lib.get("Path") or []
                if isinstance(raw_paths, str):
                    raw_paths = [raw_paths]
                for path in raw_paths:
                    normalized = str(path).replace("\\", "/").lower().rstrip("/")
                    if normalized:
                        library_paths.append(normalized)
        if not matched_names and not library_paths and not library_ids:
            logger.warning(
                f"媒体库黑名单未匹配到Emby虚拟库：server={server_name or '-'}，"
                f"规则={','.join(sorted(token_set))}"
            )
            library_names = [str(lib.get("Name") or "-") for lib in libraries[:30]]
            logger.warning(
                f"Emby虚拟库候选（最多30条）：{library_names if library_names else '[]'}"
            )
        return configured_name_tokens, library_paths, library_ids

    @staticmethod
    def _is_blacklisted_library_item(
            item: Dict[str, Any],
            blacklisted_paths: List[str],
            blacklisted_library_ids: Optional[set] = None,
            blacklisted_library_names: Optional[set] = None) -> bool:
        # 先按祖先库ID匹配，兼容Path为空或路径不一致场景
        if blacklisted_library_ids:
            ancestor_ids = {str(ancestor).strip().lower() for ancestor in (item.get("AncestorIds") or []) if ancestor}
            if ancestor_ids.intersection(blacklisted_library_ids):
                return True

        item_path = str(item.get("Path") or "").replace("\\", "/").lower().rstrip("/")
        if item_path:
            for base in blacklisted_paths:
                if item_path.startswith(base):
                    return True
            # 名称兜底，处理部分Emby场景下虚拟库路径与条目路径前缀无法直接对齐
            if blacklisted_library_names:
                for library_name in blacklisted_library_names:
                    lib_lower = str(library_name).strip().lower()
                    if not lib_lower:
                        continue
                    if f"/{lib_lower}/" in item_path or item_path.endswith(f"/{lib_lower}"):
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
            playback_ticks = self._parse_playback_ticks(item)
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
                    pass
                else:
                    reason_counter["missing_last_played_kept"] += 1
            record = series_map.get(key)
            record_last_played = (record or {}).get("last_played", datetime.datetime.min)
            record_ticks = int((record or {}).get("playback_ticks") or 0)
            current_last_played = last_played_dt or datetime.datetime.min
            should_replace = False
            if not record:
                should_replace = True
            elif current_last_played > record_last_played:
                should_replace = True
            elif current_last_played == record_last_played and playback_ticks > record_ticks:
                should_replace = True
            if should_replace:
                series_map[key] = {
                    "series_id": series_id,
                    "season": int(season) if season else None,
                    "episode": int(episode) if episode else None,
                    "last_played": current_last_played,
                    "playback_ticks": playback_ticks,
                    "user": item.get("_mp_user")
                }
            else:
                reason_counter["duplicate_older"] += 1
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
    def _parse_playback_ticks(item: Dict[str, Any]) -> int:
        ticks = (item.get("UserData") or {}).get("PlaybackPositionTicks")
        try:
            return int(ticks or 0)
        except Exception:
            return 0

    @staticmethod
    def _resume_item_desc(item: Dict[str, Any]) -> str:
        user_name = item.get("_mp_user")
        series_name = item.get("SeriesName") or item.get("Name") or "未知剧集"
        episode_name = item.get("Name") or "-"
        season = item.get("ParentIndexNumber")
        episode = item.get("IndexNumber")
        item_id = item.get("Id")
        user_prefix = f"user={user_name}，" if user_name else ""
        return f"{user_prefix}title={series_name}，episode={episode_name}，S{season}E{episode}，item_id={item_id}"

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
    def _is_ended(mediainfo: MediaInfo) -> bool:
        ended_status = {"ended", "canceled", "cancelled", "完结", "已完结"}
        status = (mediainfo.status or "").strip().lower()
        return status in ended_status

    def _resolve_season_state(
            self,
            mediainfo: MediaInfo,
            current_season: int,
            no_exists: Dict[int, Dict[int, NotExistMediaInfo]]) -> Tuple[bool, str]:
        # 规则：优先按 TMDB 全剧完结状态；否则按“当前季是否仍在更新”判定，避免多季剧误判。
        if self._is_ended(mediainfo):
            return True, "tmdb_status_ended"

        status = (mediainfo.status or "").strip().lower()
        next_season = self._next_episode_season(mediainfo)
        has_missing = bool(no_exists)

        if next_season:
            if next_season == current_season:
                return False, "next_episode_in_current_season"
            if next_season > current_season:
                if has_missing:
                    return False, "next_episode_in_future_season_but_current_missing"
                return True, "next_episode_in_future_season_current_complete"

        if has_missing:
            return False, "current_season_has_missing"

        if status in {"continuing", "returning series", "upcoming", "更新中", "连载中"}:
            # 全剧可能仍是 Returning Series，但当前季无缺失且无下一集信息时，按当前季已完结处理。
            return True, "tmdb_active_but_current_complete"

        return True, "fallback_current_complete"

    @staticmethod
    def _next_episode_season(mediainfo: MediaInfo) -> Optional[int]:
        next_ep = mediainfo.next_episode_to_air or {}
        if not isinstance(next_ep, dict):
            return None
        season_number = next_ep.get("season_number")
        try:
            return int(season_number) if season_number is not None else None
        except Exception:
            return None

    @staticmethod
    def _next_episode_number(mediainfo: MediaInfo) -> Optional[int]:
        next_ep = mediainfo.next_episode_to_air or {}
        if not isinstance(next_ep, dict):
            return None
        episode_number = next_ep.get("episode_number")
        try:
            return int(episode_number) if episode_number is not None else None
        except Exception:
            return None

    def _trim_no_exists_for_current_airing(
            self,
            no_exists: Dict[int, Dict[int, NotExistMediaInfo]],
            mediainfo: MediaInfo,
            current_season: int) -> Dict[int, Dict[int, NotExistMediaInfo]]:
        if not no_exists:
            return {}
        aired_upper = self._current_season_aired_upper_bound(mediainfo=mediainfo, current_season=current_season)
        if aired_upper is None:
            return no_exists
        trimmed: Dict[int, Dict[int, NotExistMediaInfo]] = {}
        for media_key, season_map in (no_exists or {}).items():
            for season, info in (season_map or {}).items():
                season_no_exists = trimmed.setdefault(media_key, {})
                if int(season) != int(current_season):
                    season_no_exists[season] = info
                    continue
                episodes = sorted(set(info.episodes or []))
                if episodes:
                    actionable_episodes = [ep for ep in episodes if ep <= aired_upper]
                else:
                    actionable_episodes = list(range(1, aired_upper + 1)) if aired_upper > 0 else []
                if not actionable_episodes:
                    continue
                season_no_exists[season] = NotExistMediaInfo(
                    season=info.season,
                    episodes=actionable_episodes,
                    total_episode=min(info.total_episode or aired_upper, aired_upper),
                    start_episode=min(actionable_episodes)
                )
            if media_key in trimmed and not trimmed.get(media_key):
                trimmed.pop(media_key, None)
        return trimmed

    def _current_season_aired_upper_bound(self, mediainfo: MediaInfo, current_season: int) -> Optional[int]:
        next_season = self._next_episode_season(mediainfo)
        next_episode = self._next_episode_number(mediainfo)
        season_episodes = sorted(set((mediainfo.seasons or {}).get(current_season) or []))
        season_max = max(season_episodes) if season_episodes else None
        if next_season == current_season and next_episode:
            return max(next_episode - 1, 0)
        if next_season and next_season > current_season:
            return season_max
        if self._is_ended(mediainfo):
            return season_max
        return None

    def _accelerate_series(self, search_chain: SearchChain, download_chain: DownloadChain,
                           mediainfo: MediaInfo, meta: MetaInfo, season: int) -> bool:
        mediakey = mediainfo.tmdb_id or mediainfo.douban_id
        if not mediakey:
            return False
        contexts = self._accelerate_contexts_from_cache(mediainfo=mediainfo, season=season)
        logger.info(f"{mediainfo.title_year} 追更缓存命中资源数：{len(contexts)}")
        if not contexts:
            logger.info(f"{mediainfo.title_year} 追更缓存未命中，跳过本次追更（不触发全站搜索）")
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
            logger.info(f"追更更新下载：{mediainfo.title_year} {meta.season_episode}")
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
