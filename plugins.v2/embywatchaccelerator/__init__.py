import datetime
import re
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

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
from app.db.downloadhistory_oper import DownloadHistoryOper

_lock = Lock()


class EmbyWatchAccelerator(_PluginBase):
    # 插件名称
    plugin_name = "Emby追更补全"
    # 插件描述
    plugin_desc = "基于Emby继续观看的剧集记录，自动搜索并追更更新或补全缺失剧集。"
    # 插件图标
    plugin_icon = "download.png"
    # 插件版本
    plugin_version = "1.0.37"
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
    _accelerate_warm_interval_minutes: int = 180
    _accelerate_cold_interval_hours: int = 24
    _accelerate_cold_enabled: bool = True
    _backfill_interval_hours: int = 6
    _resume_limit: int = 50
    _history_limit: int = 80
    _recent_added_limit: int = 40
    _resume_days: int = 30
    _user_whitelist: str = ""
    _user_blacklist: str = ""
    _library_blacklist: str = ""
    _backfill_stats_only: bool = False
    _max_log_records: int = 200
    _run_once: bool = False
    _candidate_retention_days: int = 30
    _candidate_pool_clear: bool = False
    _candidate_pool_remove: str = ""
    _candidate_pool_pin_add: str = ""
    _candidate_pool_pin_remove: str = ""
    _enable_airtime_gate: bool = True
    _airtime_timezone: str = "Asia/Shanghai"
    _airtime_buffer_minutes: int = 30
    _airtime_fallback_hour: int = 20
    _airtime_probe_interval_hours: int = 8

    def init_plugin(self, config: dict = None):
        # 停止现有任务
        self.stop_service()

        if config:
            self._enabled = bool(config.get("enabled"))
            self._accelerate_interval_minutes = int(config.get("accelerate_interval_minutes") or 10)
            self._accelerate_warm_interval_minutes = int(config.get("accelerate_warm_interval_minutes") or 180)
            self._accelerate_cold_interval_hours = int(config.get("accelerate_cold_interval_hours") or 24)
            self._accelerate_cold_enabled = bool(config.get("accelerate_cold_enabled", True))
            self._backfill_interval_hours = int(config.get("backfill_interval_hours") or 6)
            self._resume_limit = int(config.get("resume_limit") or 50)
            self._history_limit = int(config.get("history_limit") or 80)
            self._recent_added_limit = int(config.get("recent_added_limit") or 40)
            self._resume_days = int(config.get("resume_days") or 30)
            self._user_whitelist = (config.get("user_whitelist") or "").strip()
            self._user_blacklist = (config.get("user_blacklist") or "").strip()
            self._library_blacklist = (config.get("library_blacklist") or "").strip()
            self._backfill_stats_only = bool(config.get("backfill_stats_only"))
            self._run_once = bool(config.get("run_once"))
            self._candidate_retention_days = int(config.get("candidate_retention_days") or 30)
            self._candidate_pool_clear = bool(config.get("candidate_pool_clear"))
            self._candidate_pool_remove = (config.get("candidate_pool_remove") or "").strip()
            self._candidate_pool_pin_add = (config.get("candidate_pool_pin_add") or "").strip()
            self._candidate_pool_pin_remove = (config.get("candidate_pool_pin_remove") or "").strip()
            self._enable_airtime_gate = bool(config.get("enable_airtime_gate", True))
            self._airtime_timezone = str(config.get("airtime_timezone") or "Asia/Shanghai").strip() or "Asia/Shanghai"
            self._airtime_buffer_minutes = int(config.get("airtime_buffer_minutes") or 30)
            self._airtime_fallback_hour = int(config.get("airtime_fallback_hour") or 20)
            self._airtime_probe_interval_hours = int(config.get("airtime_probe_interval_hours") or 8)

        config_changed = False
        if self._candidate_pool_clear or self._candidate_pool_remove or self._candidate_pool_pin_add or self._candidate_pool_pin_remove:
            removed = self._manage_candidate_pool(
                clear_all=self._candidate_pool_clear,
                remove_spec=self._candidate_pool_remove,
                pin_add_spec=self._candidate_pool_pin_add,
                pin_remove_spec=self._candidate_pool_pin_remove
            )
            logger.info(f"候选池管理操作完成，移除条目数：{removed}")
            self._append_log(f"候选池管理操作完成，移除条目数：{removed}")
            self._candidate_pool_clear = False
            self._candidate_pool_remove = ""
            self._candidate_pool_pin_add = ""
            self._candidate_pool_pin_remove = ""
            config_changed = True

        if self._run_once:
            if self._enabled:
                self._append_log("检测到立即运行开关，开始执行一次追更任务")
                self._process(mode="accelerate")
            else:
                self._append_log("立即运行开关已开启，但插件未启用，跳过执行", "WARNING")
            self._run_once = False
            config_changed = True
        if config_changed:
            self.update_config(self._current_config())

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
        services = [
            {
                "id": "EmbyWatchAccelerateHot",
                "name": "Emby追更（热门池）",
                "trigger": "interval",
                "func": self._run_accelerate_hot,
                "kwargs": {"minutes": max(self._accelerate_interval_minutes, 1)}
            },
            {
                "id": "EmbyWatchAccelerateWarm",
                "name": "Emby追更（温池）",
                "trigger": "interval",
                "func": self._run_accelerate_warm,
                "kwargs": {"minutes": max(self._accelerate_warm_interval_minutes, 1)}
            },
            {
                "id": "EmbyWatchAccelerateCold",
                "name": "Emby追更（冷池）",
                "trigger": "interval",
                "func": self._run_accelerate_cold,
                "kwargs": {"hours": max(self._accelerate_cold_interval_hours, 1)}
            } if self._accelerate_cold_enabled else None,
            {
                "id": "EmbyWatchBackfill",
                "name": "Emby继续观看追更（补全）",
                "trigger": "interval",
                "func": self._run_backfill,
                "kwargs": {"hours": max(self._backfill_interval_hours, 1)}
            }
        ]
        return [item for item in services if item]

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
                                        "content": [{"component": "VTextField", "props": {"model": "accelerate_warm_interval_minutes", "label": "温池间隔（分钟）", "type": "number", "min": 1}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "accelerate_cold_interval_hours", "label": "冷池间隔（小时）", "type": "number", "min": 1}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VSwitch", "props": {"model": "accelerate_cold_enabled", "label": "启用冷池"}}]
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
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "candidate_retention_days", "label": "候选池保留天数", "type": "number", "min": 1}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "history_limit", "label": "播放历史读取数量", "type": "number", "min": 1}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "recent_added_limit", "label": "最近新增读取数量", "type": "number", "min": 1}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VSwitch", "props": {"model": "enable_airtime_gate", "label": "启用更新时间门控"}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "airtime_timezone", "label": "更新时间时区（IANA）"}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "airtime_buffer_minutes", "label": "更新时间缓冲（分钟）", "type": "number", "min": 0}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "airtime_fallback_hour", "label": "无精确时间默认小时", "type": "number", "min": 0, "max": 23}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 3},
                                        "content": [{"component": "VTextField", "props": {"model": "airtime_probe_interval_hours", "label": "无下一集信息兜底探测（小时）", "type": "number", "min": 1}}]
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
                        "component": "VCard",
                        "props": {"variant": "outlined", "class": "pa-3 mb-3"},
                        "content": [
                            {"component": "VCardTitle", "text": "候选池管理"},
                            {"component": "VDivider", "props": {"class": "mb-2"}},
                            {
                                "component": "VRow",
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 4},
                                        "content": [{"component": "VSwitch", "props": {"model": "candidate_pool_clear", "label": "保存后清空候选池"}}]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 8},
                                        "content": [{
                                            "component": "VTextarea",
                                            "props": {
                                                "model": "candidate_pool_remove",
                                                "label": "移除候选（每行：服务器名:SeriesId:Season，如 Emby:1282194:1）",
                                                "rows": 3,
                                                "autoGrow": True
                                            }
                                        }]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [{
                                            "component": "VTextarea",
                                            "props": {
                                                "model": "candidate_pool_pin_add",
                                                "label": "钉住候选（每行：服务器名:SeriesId:Season）",
                                                "rows": 2,
                                                "autoGrow": True
                                            }
                                        }]
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "md": 6},
                                        "content": [{
                                            "component": "VTextarea",
                                            "props": {
                                                "model": "candidate_pool_pin_remove",
                                                "label": "取消钉住（每行：服务器名:SeriesId:Season）",
                                                "rows": 2,
                                                "autoGrow": True
                                            }
                                        }]
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
                            "text": "追更任务会联合“继续观看 + 候选池”处理。可在候选池管理里按条目移除或清空。"
                        }
                    }
                ]
            }
        ], {
            "enabled": False,
            "accelerate_interval_minutes": 10,
            "accelerate_warm_interval_minutes": 180,
            "accelerate_cold_interval_hours": 24,
            "accelerate_cold_enabled": True,
            "backfill_interval_hours": 6,
            "resume_limit": 50,
            "history_limit": 80,
            "recent_added_limit": 40,
            "resume_days": 30,
            "candidate_retention_days": 30,
            "enable_airtime_gate": True,
            "airtime_timezone": "Asia/Shanghai",
            "airtime_buffer_minutes": 30,
            "airtime_fallback_hour": 20,
            "airtime_probe_interval_hours": 8,
            "user_whitelist": "",
            "user_blacklist": "",
            "library_blacklist": "",
            "backfill_stats_only": False,
            "run_once": False,
            "candidate_pool_clear": False,
            "candidate_pool_remove": "",
            "candidate_pool_pin_add": "",
            "candidate_pool_pin_remove": ""
        }

    def get_page(self) -> Optional[List[dict]]:
        stats = self.get_data("last_stats") or {}
        changed = False
        if self._migrate_stats_user_alias(stats):
            changed = True
        if self._merge_candidate_pool_into_stats(stats):
            changed = True
        if self._migrate_stats_user_alias(stats):
            changed = True
        if self._hydrate_stats_posters(stats):
            changed = True
        if changed:
            self.save_data("last_stats", stats)
        user_stats = stats.get("user_stats") or {}
        user_cards = []
        sorted_users = sorted(
            user_stats.keys(),
            key=lambda name: (1 if name == "最近入库" else 0, str(name).lower())
        )
        for user_name in sorted_users:
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
                        "props": {"cols": 12, "class": "pa-0"},
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
                        "props": {"cols": 12, "class": "pa-0 mt-4"},
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
                            "props": {"class": "d-flex justify-space-between align-end mb-3"},
                            "content": [
                                {
                                    "component": "div",
                                    "props": {"class": "text-h5 font-weight-bold", "style": "line-height:1;"},
                                    "text": user_name
                                },
                                {
                                    "component": "div",
                                    "props": {"class": "d-flex align-end"},
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
                        {
                            "component": "VRow",
                            "content": mode_blocks
                        }
                    ]
                }
            )
        if user_cards:
            return user_cards + self._build_candidate_pool_page()
        return [
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "pa-4", "style": "border-color:#e9edf4;"},
                "content": [{"component": "VCardText", "text": "暂无用户数据"}]
            }
        ] + self._build_candidate_pool_page()

    @staticmethod
    def _normalize_user_label(user_name: Optional[str]) -> str:
        raw = str(user_name or "").strip()
        if not raw:
            return "未知用户"
        if raw.lower() == "system":
            return "最近入库"
        return raw

    def _migrate_stats_user_alias(self, stats: Dict[str, Any]) -> bool:
        user_stats = stats.get("user_stats") or {}
        if not isinstance(user_stats, dict):
            return False
        alias_keys = [k for k in list(user_stats.keys()) if self._normalize_user_label(k) == "最近入库" and k != "最近入库"]
        if "system" in user_stats and "system" not in alias_keys:
            alias_keys.append("system")
        if not alias_keys:
            return False
        merged_source = {}
        for key in alias_keys:
            bucket = user_stats.pop(key, None) or {}
            for k, v in bucket.items():
                if k in ("track_items", "backfill_items"):
                    merged_source[k] = (merged_source.get(k) or []) + (v or [])
                else:
                    merged_source[k] = int(merged_source.get(k) or 0) + int(v or 0)
        alias_key = "最近入库"
        existing = user_stats.get(alias_key) or {
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
        for key in ("resume_items", "series_items", "processed_series", "track_attempts",
                    "track_downloads", "backfill_attempts", "backfill_downloads"):
            existing[key] = int(existing.get(key) or 0) + int(merged_source.get(key) or 0)
        existing["track_items"] = (existing.get("track_items") or []) + (merged_source.get("track_items") or [])
        existing["backfill_items"] = (existing.get("backfill_items") or []) + (merged_source.get("backfill_items") or [])
        user_stats[alias_key] = existing
        stats["user_stats"] = user_stats
        return True

    def _merge_candidate_pool_into_stats(self, stats: Dict[str, Any]) -> bool:
        all_servers = self.get_data(self._candidate_pool_storage_key()) or {}
        if not isinstance(all_servers, dict) or not all_servers:
            return False
        changed = False
        user_stats = stats.setdefault("user_stats", {})
        for server_name, pool in all_servers.items():
            if not isinstance(pool, dict) or not pool:
                continue
            for _, item in pool.items():
                if not isinstance(item, dict):
                    continue
                series_id = str(item.get("series_id") or "").strip()
                season = item.get("season")
                if not series_id or season is None:
                    continue
                user_name = self._normalize_user_label(item.get("user"))
                bucket = self._get_user_bucket(stats, user_name)
                track_items = bucket.setdefault("track_items", [])
                key = f"{server_name}:{series_id}:{int(season)}"
                idx = None
                for i, exist in enumerate(track_items):
                    exist_key = f"{exist.get('server') or ''}:{exist.get('series_id') or ''}:{int(exist.get('season') or 0)}"
                    if exist_key == key:
                        idx = i
                        break
                next_time = self._candidate_next_track_time(item)
                row = {
                    "title": item.get("series_name") or item.get("title") or "未知剧集",
                    "year": item.get("year"),
                    "season": int(season),
                    "result": self._display_track_result(item.get("last_track_result") or "候选池"),
                    "poster": item.get("poster") or "",
                    "poster_source": item.get("poster_source") or "candidate_pool",
                    "series_id": series_id,
                    "server": server_name,
                    "type": item.get("type") or "电视剧",
                    "time": next_time
                }
                if idx is None:
                    track_items.append(row)
                    changed = True
                else:
                    old = track_items[idx]
                    merged = dict(old)
                    merged.update({k: v for k, v in row.items() if v not in (None, "")})
                    merged["time"] = next_time
                    if merged != old:
                        track_items[idx] = merged
                        changed = True
                # 确保用户别名统一
                if user_name not in user_stats and self._normalize_user_label(user_name) == "最近入库":
                    changed = True
        return changed

    @staticmethod
    def _display_track_result(raw_result: str) -> str:
        text = str(raw_result or "").strip()
        if not text:
            return "候选池"
        if text.startswith("门控跳过："):
            return "未到更新时间"
        return text

    def _build_candidate_pool_page(self) -> List[dict]:
        all_servers = self.get_data(self._candidate_pool_storage_key()) or {}
        if not isinstance(all_servers, dict) or not all_servers:
            return []
        all_pins = self._load_candidate_pins()
        server_cards = []
        for server_name in sorted(all_servers.keys()):
            pool = all_servers.get(server_name) or {}
            if not isinstance(pool, dict) or not pool:
                continue
            pin_set = all_pins.get(server_name.lower(), set())
            items = []
            for key, item in sorted(pool.items(), key=lambda kv: str((kv[1] or {}).get("last_seen_at") or ""), reverse=True):
                pin_text = " | 钉住=是" if key in pin_set else ""
                tier_text = self._candidate_tier(self._parse_last_played(str(item.get("last_seen_at") or "")))
                items.append({
                    "component": "VListItem",
                    "props": {"density": "compact"},
                    "content": [
                        {
                            "component": "VListItemTitle",
                            "text": f"{item.get('series_name') or '未知剧集'}  S{item.get('season') or '-'}"
                        },
                        {
                            "component": "VListItemSubtitle",
                            "text": (
                                f"SeriesId={item.get('series_id') or '-'} | "
                                f"用户={self._normalize_user_label(item.get('user'))} | "
                                f"分层={tier_text} | "
                                f"最近入池={item.get('last_seen_at') or '-'} | "
                                f"键={key}{pin_text}"
                            )
                        }
                    ]
                })
            server_cards.append(
                {
                    "component": "VCard",
                    "props": {"variant": "outlined", "class": "pa-3 mt-3"},
                    "content": [
                        {"component": "VCardTitle", "text": f"追更候选池 - {server_name}"},
                        {"component": "VCardSubtitle", "text": f"条目数：{len(pool)}"},
                        {"component": "VDivider", "props": {"class": "mb-2"}},
                        {"component": "VList", "props": {"lines": "two"}, "content": items[:100]}
                    ]
                }
            )
        return server_cards

    def _current_config(self) -> Dict[str, Any]:
        return {
            "enabled": self._enabled,
            "accelerate_interval_minutes": self._accelerate_interval_minutes,
            "accelerate_warm_interval_minutes": self._accelerate_warm_interval_minutes,
            "accelerate_cold_interval_hours": self._accelerate_cold_interval_hours,
            "accelerate_cold_enabled": self._accelerate_cold_enabled,
            "backfill_interval_hours": self._backfill_interval_hours,
            "resume_limit": self._resume_limit,
            "history_limit": self._history_limit,
            "recent_added_limit": self._recent_added_limit,
            "resume_days": self._resume_days,
            "candidate_retention_days": self._candidate_retention_days,
            "enable_airtime_gate": self._enable_airtime_gate,
            "airtime_timezone": self._airtime_timezone,
            "airtime_buffer_minutes": self._airtime_buffer_minutes,
            "airtime_fallback_hour": self._airtime_fallback_hour,
            "airtime_probe_interval_hours": self._airtime_probe_interval_hours,
            "user_whitelist": self._user_whitelist,
            "user_blacklist": self._user_blacklist,
            "library_blacklist": self._library_blacklist,
            "backfill_stats_only": self._backfill_stats_only,
            "run_once": self._run_once,
            "candidate_pool_clear": self._candidate_pool_clear,
            "candidate_pool_remove": self._candidate_pool_remove,
            "candidate_pool_pin_add": self._candidate_pool_pin_add,
            "candidate_pool_pin_remove": self._candidate_pool_pin_remove
        }

    def stop_service(self):
        pass

    def _run_accelerate(self):
        self._process(mode="accelerate", tier="hot")

    def _run_accelerate_hot(self):
        self._process(mode="accelerate", tier="hot")

    def _run_accelerate_warm(self):
        self._process(mode="accelerate", tier="warm")

    def _run_accelerate_cold(self):
        self._process(mode="accelerate", tier="cold")

    def _run_backfill(self):
        self._process(mode="backfill", tier=None)

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
        user_key = EmbyWatchAccelerator._normalize_user_label(user_name)
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
    def _parse_stat_item_time(item: Dict[str, Any]) -> datetime.datetime:
        raw = str(item.get("time") or "").strip()
        if not raw:
            return datetime.datetime.min
        try:
            return datetime.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.datetime.min

    @staticmethod
    def _build_stat_cards(items: List[Dict[str, Any]], placeholder_poster: str) -> List[dict]:
        cards = []
        for item in (items or []):
            cards.append({
                "component": "VCol",
                "props": {"cols": 12, "sm": 6, "md": 4, "lg": 3, "class": "px-2 pb-4"},
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "tonal", "class": "pa-2 h-100", "style": "max-width:332px;"},
                        "content": [
                            {
                                "component": "div",
                                "props": {"class": "d-flex flex-row flex-nowrap align-center"},
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
                                        "props": {"class": "align-self-center"},
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
        return cards

    def _build_user_mode_block(self, title: str, attempts: int, downloads: int, items: List[Dict[str, Any]]) -> List[dict]:
        placeholder_poster = (
            "data:image/svg+xml;utf8,"
            "<svg xmlns='http://www.w3.org/2000/svg' width='240' height='360' viewBox='0 0 240 360'>"
            "<rect width='240' height='360' fill='%23eceff1'/>"
            "<text x='120' y='178' text-anchor='middle' fill='%2390a4ae' font-size='20'>No Cover</text>"
            "</svg>"
        )
        sorted_items = sorted(items or [], key=self._parse_stat_item_time, reverse=True)
        preview_items = sorted_items[:12]
        remain_items = sorted_items[12:]
        preview_cards = self._build_stat_cards(preview_items, placeholder_poster)
        remain_cards = self._build_stat_cards(remain_items, placeholder_poster)

        content = [
            {
                "component": "div",
                "props": {"class": "position-relative mb-2", "style": "height:24px;"},
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
        if preview_cards:
            content.append({"component": "VRow", "props": {"class": "ma-0"}, "content": preview_cards})
            if remain_items:
                content.append({
                    "component": "VExpansionPanels",
                    "props": {"variant": "accordion", "class": "mt-1"},
                    "content": [
                        {
                            "component": "VExpansionPanel",
                            "content": [
                                {
                                    "component": "VExpansionPanelTitle",
                                    "text": "展开查看更多"
                                },
                                {
                                    "component": "VExpansionPanelText",
                                    "content": [
                                        {"component": "VRow", "props": {"class": "ma-0 pt-2"}, "content": remain_cards}
                                    ]
                                }
                            ]
                        }
                    ]
                })
        else:
            content.append({"component": "VCardText", "props": {"class": "pa-0 mt-4"}, "text": "暂无记录"})
        return content

    def _process(self, mode: str, tier: Optional[str] = None):
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
            "history_items": 0,
            "recent_added_items": 0,
            "series_items": 0,
            "history_series_items": 0,
            "recent_added_series_items": 0,
            "candidate_pool_items": 0,
            "merged_series_items": 0,
            "processed_series": 0,
            "accelerate_attempts": 0,
            "accelerate_downloads": 0,
            "backfill_attempts": 0,
            "backfill_downloads": 0,
            "backfill_skipped_stats_only": 0,
            "skipped_airtime_gate": 0,
            "skipped_library_blacklist": 0,
            "skipped_non_tv": 0,
            "skipped_no_mediainfo": 0,
            "skipped_no_seriesinfo": 0,
            "user_stats": {}
        }
        try:
            logger.info(f"继续观看任务开始，模式：{mode}，分层：{tier or 'all'}")
            tier_text = tier or "all"
            self._append_log(f"继续观看任务开始，模式：{mode}，分层：{tier_text}")
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
                self._process_emby_service(service.instance, mode=mode, stats=stats, server_name=name, tier=tier)
        finally:
            cost = (datetime.datetime.now() - start_time).total_seconds()
            stats["mode"] = mode
            stats["duration_seconds"] = round(cost, 2)
            stats["finished_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_data("last_stats", stats)
            logger.info(
                f"继续观看任务结束，模式：{mode}，耗时：{cost:.2f}秒，"
                f"服务器数：{stats['servers']}，继续观看条目：{stats['resume_items']}，"
                f"历史条目：{stats['history_items']}，去重剧集数：{stats['series_items']}，"
                f"历史去重剧集数：{stats['history_series_items']}，最近新增条目：{stats['recent_added_items']}，"
                f"最近新增去重剧集数：{stats['recent_added_series_items']}，候选池剧集数：{stats['candidate_pool_items']}，"
                f"联合处理剧集数：{stats['merged_series_items']}，处理剧集数：{stats['processed_series']}，"
                f"追更尝试/下载：{stats['accelerate_attempts']}/{stats['accelerate_downloads']}，"
                f"补全尝试/下载：{stats['backfill_attempts']}/{stats['backfill_downloads']}，"
                f"仅统计跳过补全：{stats['backfill_skipped_stats_only']}，"
                f"更新时间门控跳过：{stats['skipped_airtime_gate']}，"
                f"媒体库黑名单跳过：{stats['skipped_library_blacklist']}，"
                f"跳过非电视剧：{stats['skipped_non_tv']}，"
                f"跳过识别失败：{stats['skipped_no_mediainfo']}，"
                f"跳过详情失败：{stats['skipped_no_seriesinfo']}"
            )
            self._append_log(
                f"任务结束，模式：{mode}，耗时：{cost:.2f}秒，"
                f"分层：{tier or 'all'}，"
                f"候选池剧集数：{stats['candidate_pool_items']}，"
                f"追更尝试/下载：{stats['accelerate_attempts']}/{stats['accelerate_downloads']}，"
                f"补全尝试/下载：{stats['backfill_attempts']}/{stats['backfill_downloads']}，"
                f"仅统计跳过补全：{stats['backfill_skipped_stats_only']}，"
                f"更新时间门控跳过：{stats['skipped_airtime_gate']}，"
                f"媒体库黑名单跳过：{stats['skipped_library_blacklist']}"
            )
            _lock.release()

    def _process_emby_service(self, emby, mode: str, stats: Dict[str, int], server_name: str = "", tier: Optional[str] = None):
        resume_items = self._get_resume_items(emby, stats, server_name)
        if not resume_items:
            logger.info("未获取到继续观看的剧集记录，将仅使用追更候选池")
        else:
            stats["resume_items"] += len(resume_items)
        resume_series_items = self._merge_resume_series(resume_items) if resume_items else []
        stats["series_items"] += len(resume_series_items)
        history_items = self._get_history_items(emby=emby, server_name=server_name, stats=stats)
        if history_items:
            stats["history_items"] += len(history_items)
        history_series_items = self._merge_history_series(history_items) if history_items else []
        stats["history_series_items"] += len(history_series_items)
        recent_added_items = self._get_recent_added_items(emby=emby, server_name=server_name, stats=stats)
        recent_added_series_items = self._merge_recent_added_series(recent_added_items) if recent_added_items else []
        stats["recent_added_items"] = stats.get("recent_added_items", 0) + len(recent_added_items)
        stats["recent_added_series_items"] = stats.get("recent_added_series_items", 0) + len(recent_added_series_items)

        candidate_pool = self._load_candidate_pool(server_name=server_name)
        candidate_pool = self._prune_candidate_pool(candidate_pool, server_name=server_name)
        self._upsert_candidate_pool_from_resume(candidate_pool=candidate_pool, resume_series_items=resume_series_items)
        self._upsert_candidate_pool_from_history(candidate_pool=candidate_pool, history_series_items=history_series_items)
        self._upsert_candidate_pool_from_recent_added(
            candidate_pool=candidate_pool,
            recent_added_series_items=recent_added_series_items
        )
        self._apply_pins_to_pool(server_name=server_name, candidate_pool=candidate_pool)
        candidate_series_items = self._candidate_pool_to_series_items(candidate_pool=candidate_pool)
        candidate_series_items = self._filter_series_by_tier(candidate_series_items, tier=tier, mode=mode)
        stats["candidate_pool_items"] += len(candidate_series_items)

        series_items = self._merge_series_items(
            resume_series_items=resume_series_items,
            candidate_series_items=candidate_series_items
        )
        stats["merged_series_items"] += len(series_items)
        logger.info(
            f"本轮待处理剧集：继续观看={len(resume_series_items)}，"
            f"播放历史={len(history_series_items)}，最近新增={len(recent_added_series_items)}，"
            f"候选池={len(candidate_series_items)}，联合去重后={len(series_items)}，分层={tier or 'all'}"
        )
        if not series_items:
            logger.info("继续观看与追更候选池均为空，跳过本轮")
            self._save_candidate_pool(server_name=server_name, candidate_pool=candidate_pool)
            return

        download_chain = DownloadChain()
        search_chain = SearchChain()

        for item in series_items:
            series_id = item.get("series_id")
            current_season = item.get("season")
            current_user = item.get("user")
            source = item.get("_source") or "unknown"
            candidate_key = self._candidate_key(series_id=series_id, season=current_season)
            if not series_id or not current_season:
                continue
            logger.info(f"处理剧集来源：{source}，series_id={series_id}，season=S{current_season}，user={current_user or '-'}")
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
                if not bool((candidate_pool.get(candidate_key) or {}).get("pinned")):
                    candidate_pool.pop(candidate_key, None)
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
            next_ep = mediainfo.next_episode_to_air or {}
            candidate_entry = candidate_pool.get(candidate_key) or {}
            due_at_utc, _ = self._resolve_next_episode_due_at_utc(mediainfo, candidate_entry)
            candidate_entry["series_name"] = mediainfo.title
            candidate_entry["title"] = mediainfo.title
            candidate_entry["year"] = mediainfo.year
            candidate_entry["type"] = str(getattr(mediainfo.type, "value", mediainfo.type) or "电视剧")
            poster, poster_source = self._resolve_poster_url(mediainfo=mediainfo, emby=emby, series_id=series_id)
            if poster:
                candidate_entry["poster"] = poster
                candidate_entry["poster_source"] = poster_source
            candidate_entry["user"] = self._normalize_user_label(current_user)
            candidate_entry["next_episode_air_date"] = next_ep.get("air_date")
            candidate_entry["next_episode_season"] = next_ep.get("season_number")
            candidate_entry["next_episode_number"] = next_ep.get("episode_number")
            candidate_entry["next_track_at"] = due_at_utc.isoformat() if due_at_utc else ""
            candidate_pool[candidate_key] = candidate_entry
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
                else:
                    if bool((candidate_pool.get(candidate_key) or {}).get("pinned")):
                        logger.info(f"{mediainfo.title_year} 已完结且无缺失，但已钉住，保留候选池")
                    else:
                        logger.info(f"{mediainfo.title_year} 已完结且无缺失，从追更候选池移除")
                        candidate_pool.pop(candidate_key, None)
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
                if mode == "accelerate":
                    logger.info(f"{mediainfo.title_year} 更新中且存在缺失，补全后继续执行追更")
            if no_exists and not actionable_no_exists:
                logger.info(f"{mediainfo.title_year} 当前缺失均为未播集，跳过补全并进入追更策略")

            if mode == "accelerate":
                allow_track, gate_reason, next_track_at_utc = self._should_run_track_by_airtime_gate(
                    mediainfo=mediainfo,
                    candidate_item=candidate_pool.get(candidate_key) or {},
                    tier=tier
                )
                candidate_entry = candidate_pool.get(candidate_key) or {}
                candidate_entry["next_track_at"] = next_track_at_utc.isoformat() if next_track_at_utc else ""
                candidate_entry["default_interval_minutes"] = self._tier_interval_minutes(tier=tier)
                candidate_pool[candidate_key] = candidate_entry
                if not allow_track:
                    stats["skipped_airtime_gate"] += 1
                    candidate_entry = candidate_pool.get(candidate_key) or {}
                    candidate_entry["last_track_result"] = f"门控跳过：{gate_reason}"
                    candidate_pool[candidate_key] = candidate_entry
                    logger.info(f"{mediainfo.title_year} 追更跳过：{gate_reason}")
                    continue
                logger.info(f"{mediainfo.title_year} 状态：{status_text}，执行追更更新（缓存匹配）")
                stats["accelerate_attempts"] += 1
                user_bucket["track_attempts"] += 1
                now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                candidate_entry = candidate_pool.get(candidate_key) or {}
                candidate_entry["last_track_at"] = now_str
                candidate_entry["last_track_next_episode"] = (
                    f"{next_ep.get('season_number') or '-'}:{next_ep.get('episode_number') or '-'}:{next_ep.get('air_date') or '-'}"
                )
                candidate_pool[candidate_key] = candidate_entry
                if self._accelerate_series(search_chain, download_chain, mediainfo, meta, current_season):
                    stats["accelerate_downloads"] += 1
                    user_bucket["track_downloads"] += 1
                    self._update_learned_hit_minutes(candidate_entry)
                    self._append_user_media_result(
                        stats=stats, user_name=current_user, kind="track",
                        mediainfo=mediainfo, season=current_season, result="已下载",
                        series_id=series_id, server_name=server_name, emby=emby
                    )
                    candidate_entry = candidate_pool.get(candidate_key) or {}
                    candidate_entry["last_track_result"] = "已下载"
                    candidate_pool[candidate_key] = candidate_entry
                else:
                    self._append_user_media_result(
                        stats=stats, user_name=current_user, kind="track",
                        mediainfo=mediainfo, season=current_season, result="未命中资源",
                        series_id=series_id, server_name=server_name, emby=emby
                    )
                    candidate_entry = candidate_pool.get(candidate_key) or {}
                    candidate_entry["last_track_result"] = "未命中资源"
                    candidate_pool[candidate_key] = candidate_entry
        self._save_candidate_pool(server_name=server_name, candidate_pool=candidate_pool)

    @staticmethod
    def _candidate_key(series_id: Optional[str], season: Optional[int]) -> str:
        return f"{series_id}:{season}"

    @staticmethod
    def _candidate_pool_storage_key() -> str:
        return "track_candidate_pool"

    @staticmethod
    def _candidate_pool_pin_storage_key() -> str:
        return "track_candidate_pins"

    def _load_candidate_pool(self, server_name: str) -> Dict[str, Dict[str, Any]]:
        all_servers = self.get_data(self._candidate_pool_storage_key()) or {}
        server_key = (server_name or "default").strip() or "default"
        server_pool = all_servers.get(server_key) or {}
        if isinstance(server_pool, dict):
            normalized = dict(server_pool)
            changed = False
            for key, item in normalized.items():
                if not isinstance(item, dict):
                    continue
                normalized_user = self._normalize_user_label(item.get("user"))
                if normalized_user != item.get("user"):
                    item["user"] = normalized_user
                    normalized[key] = item
                    changed = True
            if changed:
                all_servers[server_key] = normalized
                self.save_data(self._candidate_pool_storage_key(), all_servers)
            return normalized
        return {}

    def _save_candidate_pool(self, server_name: str, candidate_pool: Dict[str, Dict[str, Any]]) -> None:
        all_servers = self.get_data(self._candidate_pool_storage_key()) or {}
        if not isinstance(all_servers, dict):
            all_servers = {}
        server_key = (server_name or "default").strip() or "default"
        all_servers[server_key] = candidate_pool
        self.save_data(self._candidate_pool_storage_key(), all_servers)

    def _load_candidate_pins(self) -> Dict[str, set]:
        raw = self.get_data(self._candidate_pool_pin_storage_key()) or {}
        result: Dict[str, set] = {}
        if not isinstance(raw, dict):
            return result
        for server, keys in raw.items():
            if isinstance(keys, list):
                result[str(server).lower()] = set(str(key) for key in keys if key)
        return result

    def _save_candidate_pins(self, pins: Dict[str, set]) -> None:
        payload = {server: sorted(list(keys)) for server, keys in pins.items() if keys}
        self.save_data(self._candidate_pool_pin_storage_key(), payload)

    def _apply_pins_to_pool(self, server_name: str, candidate_pool: Dict[str, Dict[str, Any]]) -> None:
        pins = self._load_candidate_pins()
        pin_set = pins.get((server_name or "").lower(), set()) or set()
        for key, item in candidate_pool.items():
            item["pinned"] = key in pin_set

    def _prune_candidate_pool(self, candidate_pool: Dict[str, Dict[str, Any]], server_name: str = "") -> Dict[str, Dict[str, Any]]:
        now = datetime.datetime.now()
        pin_set = self._load_candidate_pins().get((server_name or "").lower(), set()) or set()
        pruned: Dict[str, Dict[str, Any]] = {}
        removed = 0
        for key, item in (candidate_pool or {}).items():
            if key in pin_set:
                item["pinned"] = True
                pruned[key] = item
                continue
            last_seen_str = str(item.get("last_seen_at") or "").strip()
            last_seen_dt = self._parse_last_played(last_seen_str) if last_seen_str else None
            if not last_seen_dt:
                last_seen_dt = now
            if (now - last_seen_dt).days > max(self._candidate_retention_days, 1):
                removed += 1
                continue
            pruned[key] = item
        if removed:
            logger.info(f"追更候选池清理过期条目：{removed}")
        return pruned

    def _upsert_candidate_pool_from_resume(
            self,
            candidate_pool: Dict[str, Dict[str, Any]],
            resume_series_items: List[Dict[str, Any]]) -> None:
        now = datetime.datetime.now().isoformat()
        for item in resume_series_items:
            series_id = str(item.get("series_id") or "").strip()
            season = item.get("season")
            if not series_id or season is None:
                continue
            key = self._candidate_key(series_id=series_id, season=season)
            existing = candidate_pool.get(key) or {}
            candidate_pool[key] = {
                "series_id": series_id,
                "series_name": item.get("series_name") or existing.get("series_name"),
                "season": int(season),
                "episode": item.get("episode") or existing.get("episode"),
                "user": self._normalize_user_label(item.get("user") or existing.get("user")),
                "last_played": (item.get("last_played") or datetime.datetime.min).isoformat()
                if not existing.get("last_played") else existing.get("last_played"),
                "playback_ticks": int(item.get("playback_ticks") or existing.get("playback_ticks") or 0),
                "last_seen_at": now,
                "last_track_at": existing.get("last_track_at"),
                "last_track_next_episode": existing.get("last_track_next_episode"),
                "pinned": bool(existing.get("pinned"))
            }

    def _upsert_candidate_pool_from_history(
            self,
            candidate_pool: Dict[str, Dict[str, Any]],
            history_series_items: List[Dict[str, Any]]) -> None:
        now = datetime.datetime.now().isoformat()
        for item in history_series_items:
            series_id = str(item.get("series_id") or "").strip()
            season = item.get("season")
            if not series_id or season is None:
                continue
            key = self._candidate_key(series_id=series_id, season=season)
            existing = candidate_pool.get(key) or {}
            candidate_pool[key] = {
                "series_id": series_id,
                "series_name": item.get("series_name") or existing.get("series_name"),
                "season": int(season),
                "episode": existing.get("episode") or item.get("episode"),
                "user": self._normalize_user_label(existing.get("user") or item.get("user")),
                "last_played": existing.get("last_played") or (item.get("last_played") or datetime.datetime.min).isoformat(),
                "playback_ticks": int(existing.get("playback_ticks") or item.get("playback_ticks") or 0),
                "last_seen_at": now,
                "last_track_at": existing.get("last_track_at"),
                "last_track_next_episode": existing.get("last_track_next_episode"),
                "pinned": bool(existing.get("pinned"))
            }

    def _upsert_candidate_pool_from_recent_added(
            self,
            candidate_pool: Dict[str, Dict[str, Any]],
            recent_added_series_items: List[Dict[str, Any]]) -> None:
        now = datetime.datetime.now().isoformat()
        for item in recent_added_series_items:
            series_id = str(item.get("series_id") or "").strip()
            season = item.get("season")
            if not series_id or season is None:
                continue
            key = self._candidate_key(series_id=series_id, season=season)
            existing = candidate_pool.get(key) or {}
            candidate_pool[key] = {
                "series_id": series_id,
                "series_name": item.get("series_name") or existing.get("series_name"),
                "season": int(season),
                "episode": existing.get("episode") or item.get("episode"),
                "user": self._normalize_user_label(existing.get("user") or item.get("user")),
                "last_played": existing.get("last_played") or (item.get("last_played") or datetime.datetime.min).isoformat(),
                "playback_ticks": int(existing.get("playback_ticks") or item.get("playback_ticks") or 0),
                "last_seen_at": now,
                "last_track_at": existing.get("last_track_at"),
                "last_track_next_episode": existing.get("last_track_next_episode"),
                "pinned": bool(existing.get("pinned"))
            }

    @staticmethod
    def _candidate_pool_to_series_items(candidate_pool: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        result: List[Dict[str, Any]] = []
        for _, item in (candidate_pool or {}).items():
            series_id = str(item.get("series_id") or "").strip()
            season = item.get("season")
            if not series_id or season is None:
                continue
            result.append({
                "series_id": series_id,
                "series_name": item.get("series_name"),
                "season": int(season),
                "episode": item.get("episode"),
                "last_played": datetime.datetime.min,
                "playback_ticks": int(item.get("playback_ticks") or 0),
                "user": EmbyWatchAccelerator._normalize_user_label(item.get("user")),
                "last_seen_at": item.get("last_seen_at"),
                "pinned": bool(item.get("pinned")),
                "_source": "candidate_pool"
            })
        return result

    def _filter_series_by_tier(self, series_items: List[Dict[str, Any]], tier: Optional[str], mode: str) -> List[Dict[str, Any]]:
        if mode != "accelerate" or not tier:
            return series_items
        filtered: List[Dict[str, Any]] = []
        for item in series_items:
            if item.get("pinned"):
                filtered.append(item)
                continue
            last_seen = self._parse_last_played(str(item.get("last_seen_at") or ""))
            item_tier = self._candidate_tier(last_seen)
            if item_tier == tier:
                filtered.append(item)
        return filtered

    @staticmethod
    def _candidate_tier(last_seen: Optional[datetime.datetime]) -> str:
        if not last_seen:
            return "cold"
        now = datetime.datetime.now()
        days = (now - last_seen).days
        if days <= 7:
            return "hot"
        if days <= 30:
            return "warm"
        return "cold"

    @staticmethod
    def _merge_series_items(
            resume_series_items: List[Dict[str, Any]],
            candidate_series_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for item in candidate_series_items:
            key = f"{item.get('series_id')}:{item.get('season')}"
            merged[key] = item
        for item in resume_series_items:
            key = f"{item.get('series_id')}:{item.get('season')}"
            item["_source"] = "resume"
            merged[key] = item
        return list(merged.values())

    def _manage_candidate_pool(self, clear_all: bool, remove_spec: str, pin_add_spec: str, pin_remove_spec: str) -> int:
        all_servers = self.get_data(self._candidate_pool_storage_key()) or {}
        if not isinstance(all_servers, dict):
            all_servers = {}
        pins = self._load_candidate_pins()
        removed = 0
        if clear_all:
            for _, pool in all_servers.items():
                if isinstance(pool, dict):
                    removed += len(pool)
            self.save_data(self._candidate_pool_storage_key(), {})
            self.save_data(self._candidate_pool_pin_storage_key(), {})
            return removed
        remove_rules = self._parse_candidate_remove_rules(remove_spec)
        pin_add_rules = self._parse_candidate_remove_rules(pin_add_spec)
        pin_remove_rules = self._parse_candidate_remove_rules(pin_remove_spec)
        for server_name in list(all_servers.keys()):
            pool = all_servers.get(server_name) or {}
            if not isinstance(pool, dict) or not pool:
                continue
            pin_set = pins.get(server_name.lower(), set())
            for key in list(pool.keys()):
                series_id, season = self._split_candidate_key(key)
                if not series_id or season is None:
                    continue
                if self._match_candidate_remove_rule(
                        server_name=server_name,
                        series_id=series_id,
                        season=season,
                        rules=remove_rules):
                    pool.pop(key, None)
                    pin_set.discard(key)
                    removed += 1
                    continue
                if self._match_candidate_remove_rule(server_name, series_id, season, pin_add_rules):
                    pin_set.add(key)
                if self._match_candidate_remove_rule(server_name, series_id, season, pin_remove_rules):
                    pin_set.discard(key)
            pins[server_name.lower()] = pin_set
            all_servers[server_name] = pool
        # 支持钉住尚未入池的目标，后续入池后自动生效
        for rule in pin_add_rules:
            server_key = "*" if rule.get("server") == "*" else str(rule.get("server"))
            key = self._candidate_key(rule.get("series_id"), int(rule.get("season")))
            if server_key == "*":
                for name in list(all_servers.keys()):
                    pins.setdefault(name.lower(), set()).add(key)
            else:
                pins.setdefault(server_key.lower(), set()).add(key)
        for rule in pin_remove_rules:
            server_key = "*" if rule.get("server") == "*" else str(rule.get("server"))
            key = self._candidate_key(rule.get("series_id"), int(rule.get("season")))
            if server_key == "*":
                for name in list(pins.keys()):
                    pins.setdefault(name, set()).discard(key)
            else:
                pins.setdefault(server_key.lower(), set()).discard(key)
        self.save_data(self._candidate_pool_storage_key(), all_servers)
        self._save_candidate_pins(pins)
        return removed

    @staticmethod
    def _split_candidate_key(key: str) -> Tuple[Optional[str], Optional[int]]:
        text = str(key or "").strip()
        parts = text.split(":")
        if len(parts) != 2:
            return None, None
        series_id = parts[0].strip()
        try:
            season = int(parts[1].strip())
        except Exception:
            return None, None
        return series_id, season

    @staticmethod
    def _parse_candidate_remove_rules(spec: str) -> List[Dict[str, Any]]:
        rules: List[Dict[str, Any]] = []
        for line in spec.splitlines():
            text = line.strip()
            if not text:
                continue
            parts = [part.strip() for part in text.split(":") if part.strip()]
            if len(parts) == 2:
                series_id = parts[0]
                try:
                    season = int(parts[1])
                except Exception:
                    continue
                rules.append({"server": "*", "series_id": series_id, "season": season})
            elif len(parts) == 3:
                server = parts[0]
                series_id = parts[1]
                try:
                    season = int(parts[2])
                except Exception:
                    continue
                rules.append({"server": server.lower(), "series_id": series_id, "season": season})
        return rules

    @staticmethod
    def _match_candidate_remove_rule(
            server_name: str,
            series_id: str,
            season: int,
            rules: List[Dict[str, Any]]) -> bool:
        server_l = (server_name or "").strip().lower()
        for rule in rules:
            if rule.get("series_id") != series_id:
                continue
            if int(rule.get("season")) != int(season):
                continue
            rule_server = str(rule.get("server") or "*").strip().lower()
            if rule_server == "*" or rule_server == server_l:
                return True
        return False

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

    def _get_history_items(self, emby, server_name: str = "", stats: Optional[Dict[str, int]] = None) -> List[dict]:
        users = self._get_emby_users(emby)
        if not users:
            return []
        blacklist_names, blacklist_paths, blacklist_library_ids = self._build_library_blacklist_for_server(emby, server_name)
        limit = max(self._history_limit, 1)
        per_user_limit = max(1, int((limit + len(users) - 1) / len(users)))
        all_items: List[dict] = []
        for user in users:
            user_id = user.get("Id")
            if not user_id:
                continue
            user_name = user.get("Name") or user_id
            url = (f"[HOST]emby/Users/{user_id}/Items"
                   f"?Recursive=true&IncludeItemTypes=Episode&Filters=IsPlayed"
                   f"&SortBy=DatePlayed&SortOrder=Descending&Limit={per_user_limit}"
                   "&Fields=ProviderIds,SeriesId,ParentIndexNumber,IndexNumber,ProductionYear,Path,AncestorIds,DatePlayed"
                   "&api_key=[APIKEY]")
            res = emby.get_data(url)
            if not res or res.status_code != 200:
                continue
            items = res.json().get("Items") or []
            filtered_items = []
            for episode_item in items:
                if episode_item.get("Type") != "Episode":
                    continue
                tagged_item = dict(episode_item)
                tagged_item["_mp_user"] = user_name
                if self._is_blacklisted_library_item(
                        tagged_item,
                        blacklisted_paths=blacklist_paths,
                        blacklisted_library_ids=blacklist_library_ids,
                        blacklisted_library_names=blacklist_names):
                    if stats is not None:
                        stats["skipped_library_blacklist"] = stats.get("skipped_library_blacklist", 0) + 1
                    continue
                filtered_items.append(tagged_item)
            all_items.extend(filtered_items[:per_user_limit])
            if len(all_items) >= limit:
                break
        logger.info(f"播放历史候选条目数：{len(all_items[:limit])}")
        return all_items[:limit]

    def _get_recent_added_items(self, emby, server_name: str = "", stats: Optional[Dict[str, int]] = None) -> List[dict]:
        limit = max(self._recent_added_limit, 1)
        url = ("[HOST]emby/Items"
               f"?Recursive=true&IncludeItemTypes=Episode&SortBy=DateCreated&SortOrder=Descending&Limit={limit}"
               "&Fields=ProviderIds,SeriesId,ParentIndexNumber,IndexNumber,ProductionYear,Path,AncestorIds,DateCreated"
               "&api_key=[APIKEY]")
        res = emby.get_data(url)
        if not res or res.status_code != 200:
            return []
        items = res.json().get("Items") or []
        blacklist_names, blacklist_paths, blacklist_library_ids = self._build_library_blacklist_for_server(emby, server_name)
        filtered: List[dict] = []
        for item in items:
            if item.get("Type") != "Episode":
                continue
            tagged_item = dict(item)
            tagged_item["_mp_user"] = "最近入库"
            if self._is_blacklisted_library_item(
                    tagged_item,
                    blacklisted_paths=blacklist_paths,
                    blacklisted_library_ids=blacklist_library_ids,
                    blacklisted_library_names=blacklist_names):
                if stats is not None:
                    stats["skipped_library_blacklist"] = stats.get("skipped_library_blacklist", 0) + 1
                continue
            filtered.append(tagged_item)
        logger.info(f"最近新增候选条目数：{len(filtered)}")
        return filtered

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
                    "series_name": item.get("SeriesName") or item.get("Name"),
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

    def _merge_history_series(self, items: List[dict]) -> List[dict]:
        if not items:
            return []
        series_map: Dict[str, dict] = {}
        now = datetime.datetime.now()
        skipped_missing = 0
        skipped_days = 0
        for item in items:
            series_id = item.get("SeriesId")
            season = item.get("ParentIndexNumber")
            episode = item.get("IndexNumber")
            if not series_id or not season:
                skipped_missing += 1
                continue
            last_played = (item.get("UserData") or {}).get("LastPlayedDate") or item.get("DatePlayed")
            last_played_dt = self._parse_last_played(last_played)
            if self._resume_days and last_played_dt and (now - last_played_dt).days > self._resume_days:
                skipped_days += 1
                continue
            key = f"{series_id}:{season}"
            record = series_map.get(key)
            current_dt = last_played_dt or datetime.datetime.min
            record_dt = (record or {}).get("last_played", datetime.datetime.min)
            if (not record) or current_dt >= record_dt:
                series_map[key] = {
                    "series_id": series_id,
                    "season": int(season) if season else None,
                    "episode": int(episode) if episode else None,
                    "series_name": item.get("SeriesName") or item.get("Name"),
                    "last_played": current_dt,
                    "playback_ticks": self._parse_playback_ticks(item),
                    "user": item.get("_mp_user"),
                    "_source": "history"
                }
        logger.info(
            f"播放历史去重后剧集数：{len(series_map)}，"
            f"排除缺少series/season={skipped_missing}，超出天数={skipped_days}"
        )
        return list(series_map.values())

    @staticmethod
    def _merge_recent_added_series(items: List[dict]) -> List[dict]:
        if not items:
            return []
        series_map: Dict[str, dict] = {}
        for item in items:
            series_id = item.get("SeriesId")
            season = item.get("ParentIndexNumber")
            episode = item.get("IndexNumber")
            if not series_id or not season:
                continue
            key = f"{series_id}:{season}"
            series_map[key] = {
                "series_id": series_id,
                "season": int(season) if season else None,
                "episode": int(episode) if episode else None,
                "series_name": item.get("SeriesName") or item.get("Name"),
                "last_played": datetime.datetime.min,
                "playback_ticks": 0,
                "user": "最近入库",
                "_source": "recent_added"
            }
        logger.info(f"最近新增去重后剧集数：{len(series_map)}")
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

    def _should_run_track_by_airtime_gate(
            self,
            mediainfo: MediaInfo,
            candidate_item: Dict[str, Any],
            tier: Optional[str]) -> Tuple[bool, str, Optional[datetime.datetime]]:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        next_ep = mediainfo.next_episode_to_air or {}
        next_key = f"{next_ep.get('season_number') or '-'}:{next_ep.get('episode_number') or '-'}:{next_ep.get('air_date') or '-'}"
        last_track_utc = self._parse_track_time_utc(candidate_item.get("last_track_at"))
        last_track_key = str(candidate_item.get("last_track_next_episode") or "").strip()

        if not self._enable_airtime_gate:
            next_by_interval = self._next_track_time_by_interval(last_track_utc, tier)
            if next_by_interval and now_utc < next_by_interval:
                return False, f"未到默认间隔窗口，预计执行时间={self._format_dt_in_tz(next_by_interval)}", next_by_interval
            return True, "airtime_gate_disabled", next_by_interval

        due_at_utc, due_note = self._resolve_next_episode_due_at_utc(
            mediainfo=mediainfo,
            candidate_item=candidate_item
        )
        if due_at_utc:
            if now_utc < due_at_utc:
                return False, f"未到更新时间窗口，预计执行时间={self._format_dt_in_tz(due_at_utc)}，依据={due_note}", due_at_utc
            if "default_day_start" in due_note:
                next_by_interval = self._next_track_time_by_interval(last_track_utc, tier)
                if next_by_interval and now_utc < next_by_interval:
                    return False, (
                        f"未到默认间隔窗口，预计执行时间={self._format_dt_in_tz(next_by_interval)}，依据={due_note}"
                    ), next_by_interval
                return True, f"当日按默认间隔执行追更，依据={due_note}", next_by_interval
            if last_track_utc and last_track_key == next_key and last_track_utc >= due_at_utc:
                return False, f"当前更新时间窗口已追更，最后追更时间={self._format_dt_in_tz(last_track_utc)}", due_at_utc
            return True, f"已到更新时间窗口，预计执行时间={self._format_dt_in_tz(due_at_utc)}，依据={due_note}", due_at_utc

        # 未获取下一集日期时，回退到默认追更间隔，不做轻量探测。
        next_by_interval = self._next_track_time_by_interval(last_track_utc, tier)
        if next_by_interval and now_utc < next_by_interval:
            return False, f"无下一集日期，未到默认间隔窗口，预计执行时间={self._format_dt_in_tz(next_by_interval)}", next_by_interval
        return True, "无下一集日期，按默认间隔执行追更", next_by_interval

    def _resolve_next_episode_due_at_utc(
            self,
            mediainfo: MediaInfo,
            candidate_item: Dict[str, Any]) -> Tuple[Optional[datetime.datetime], str]:
        next_ep = mediainfo.next_episode_to_air or {}
        if not isinstance(next_ep, dict):
            return None, "next_episode_missing"
        raw_air_date = str(next_ep.get("air_date") or "").strip()
        if not raw_air_date:
            return None, "next_episode_air_date_missing"

        tz = self._resolve_airtime_tz()
        date_part = raw_air_date[:10]
        try:
            air_date = datetime.datetime.strptime(date_part, "%Y-%m-%d").date()
        except Exception:
            return None, "tmdb_air_date_parse_failed"

        offset_minutes, offset_note = self._resolve_learned_hit_offset_minutes(
            mediainfo=mediainfo,
            candidate_item=candidate_item
        )
        base_local = datetime.datetime.combine(
            air_date,
            datetime.time(hour=0, minute=0, second=0),
            tzinfo=tz
        )
        total_offset = int(offset_minutes or 0) + int(self._airtime_buffer_minutes or 0)
        due_local = base_local + datetime.timedelta(minutes=total_offset)
        return due_local.astimezone(datetime.timezone.utc), (
            f"tmdb_air_date_only+offset({offset_minutes}m,{offset_note})+buffer({self._airtime_buffer_minutes}m)"
        )

    def _resolve_learned_hit_offset_minutes(
            self,
            mediainfo: MediaInfo,
            candidate_item: Dict[str, Any]) -> Tuple[int, str]:
        existing = candidate_item.get("learned_hit_minutes")
        try:
            if existing is not None:
                value = int(existing)
                if 0 <= value <= 1439:
                    return value, "candidate_pool"
        except Exception:
            pass

        history_offset = self._load_offset_from_download_history(mediainfo=mediainfo)
        if history_offset is not None:
            candidate_item["learned_hit_minutes"] = int(history_offset)
            return int(history_offset), "download_history"

        # 无历史命中时间：当天按默认间隔执行，时间点默认为当天00:00（再叠加buffer）
        return 0, "default_day_start"

    def _load_offset_from_download_history(self, mediainfo: MediaInfo) -> Optional[int]:
        rows = []
        try:
            rows = DownloadHistoryOper().get_last_by(mtype=MediaType.TV.value, tmdbid=mediainfo.tmdb_id)
        except Exception:
            rows = []
        if not rows and mediainfo.title and mediainfo.year:
            try:
                rows = DownloadHistoryOper().get_last_by(
                    title=mediainfo.title,
                    year=str(mediainfo.year),
                    season=str(mediainfo.season) if mediainfo.season is not None else None
                )
            except Exception:
                rows = []
        if not rows:
            return None

        parsed_rows: List[datetime.datetime] = []
        try:
            for row in rows:
                dt = self._parse_history_date(getattr(row, "date", None))
                if dt:
                    parsed_rows.append(dt)
        except Exception:
            parsed_rows = []
        if not parsed_rows:
            return None
        latest_day = max(dt.date() for dt in parsed_rows)
        earliest_on_latest_day = min(dt for dt in parsed_rows if dt.date() == latest_day)
        return int(earliest_on_latest_day.hour * 60 + earliest_on_latest_day.minute)

    @staticmethod
    def _parse_history_date(raw_date: Optional[str]) -> Optional[datetime.datetime]:
        raw = str(raw_date or "").strip()
        if not raw:
            return None
        fmts = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S")
        for fmt in fmts:
            try:
                return datetime.datetime.strptime(raw, fmt)
            except Exception:
                continue
        try:
            return datetime.datetime.fromisoformat(raw.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            return None

    def _next_track_time_by_interval(
            self,
            last_track_utc: Optional[datetime.datetime],
            tier: Optional[str]) -> Optional[datetime.datetime]:
        if not last_track_utc:
            return None
        minutes = self._tier_interval_minutes(tier=tier)
        return last_track_utc + datetime.timedelta(minutes=minutes)

    def _tier_interval_minutes(self, tier: Optional[str]) -> int:
        if tier == "warm":
            return max(int(self._accelerate_warm_interval_minutes or 180), 1)
        if tier == "cold":
            return max(int(self._accelerate_cold_interval_hours or 24), 1) * 60
        return max(int(self._accelerate_interval_minutes or 10), 1)

    def _update_learned_hit_minutes(self, candidate_entry: Dict[str, Any]) -> None:
        now_local = datetime.datetime.now(self._resolve_airtime_tz())
        now_minutes = int(now_local.hour * 60 + now_local.minute)
        old = candidate_entry.get("learned_hit_minutes")
        try:
            old_minutes = int(old) if old is not None else None
        except Exception:
            old_minutes = None
        if old_minutes is None or now_minutes < old_minutes:
            candidate_entry["learned_hit_minutes"] = now_minutes
            candidate_entry["learned_hit_source"] = "self_learning"

    def _resolve_airtime_tz(self):
        tz_name = str(self._airtime_timezone or "Asia/Shanghai").strip() or "Asia/Shanghai"
        try:
            return ZoneInfo(tz_name)
        except Exception:
            return ZoneInfo("Asia/Shanghai")

    @staticmethod
    def _parse_track_time_utc(raw_time: Optional[str]) -> Optional[datetime.datetime]:
        raw = str(raw_time or "").strip()
        if not raw:
            return None
        try:
            parsed = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo:
                return parsed.astimezone(datetime.timezone.utc)
            return parsed.replace(tzinfo=datetime.timezone.utc)
        except Exception:
            try:
                parsed = datetime.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
                return parsed.replace(tzinfo=datetime.timezone.utc)
            except Exception:
                return None

    def _format_dt_in_tz(self, dt_utc: datetime.datetime) -> str:
        try:
            tz = self._resolve_airtime_tz()
            return dt_utc.astimezone(tz).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return dt_utc.strftime("%Y-%m-%d %H:%M:%S")

    def _candidate_next_track_time(self, candidate_item: Dict[str, Any]) -> str:
        next_track_at = self._parse_track_time_utc(candidate_item.get("next_track_at"))
        if next_track_at:
            return self._format_dt_in_tz(next_track_at)
        if not self._enable_airtime_gate:
            last_track_utc = self._parse_track_time_utc(candidate_item.get("last_track_at"))
            if last_track_utc:
                mins = int(candidate_item.get("default_interval_minutes") or self._tier_interval_minutes(tier="hot"))
                return self._format_dt_in_tz(last_track_utc + datetime.timedelta(minutes=max(mins, 1)))
            return "-"
        next_air = str(candidate_item.get("next_episode_air_date") or "").strip()
        if next_air:
            mediainfo = MediaInfo()
            mediainfo.next_episode_to_air = {
                "air_date": next_air,
                "season_number": candidate_item.get("next_episode_season"),
                "episode_number": candidate_item.get("next_episode_number")
            }
            due_at, _ = self._resolve_next_episode_due_at_utc(mediainfo, candidate_item)
            if due_at:
                return self._format_dt_in_tz(due_at)
        last_track_utc = self._parse_track_time_utc(candidate_item.get("last_track_at"))
        if last_track_utc:
            mins = int(candidate_item.get("default_interval_minutes") or self._tier_interval_minutes(tier="hot"))
            return self._format_dt_in_tz(last_track_utc + datetime.timedelta(minutes=max(mins, 1)))
        return "-"

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
