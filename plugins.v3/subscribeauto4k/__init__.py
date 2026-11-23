"""
订阅自适应4K (SubscribeAuto4K) - MoviePilot V3 插件。

默认允许订阅通过原有优先级规则组（如 4K > 1080p）先用 1080p 兜底追更；
一旦通过以下任一信号确认某条订阅已存在可用 4K 资源：
  1. 实时事件（DownloadAdded / TransferComplete）命中 4K；
  2. 整理历史（TransferHistory）中本季已有 4K 文件落地；
  3. 本地种子缓存（TorrentsChain）中出现匹配该订阅的 4K 候选；
  4. 受控主动探查（SearchChain，受 TTL 与每批上限保护）命中满足规则组的 4K 种子；
则自动在订阅的 filter_groups 末尾追加「仅4K」过滤规则组（不覆盖原有免费/前置过滤/分类组），
并按配置将此前已下载的 1080p 等低分辨率旧集触发洗版升级，同时保留修改前快照支持回滚。
"""

import datetime
import inspect
import re
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.chain.media import MediaChain
from app.chain.search import SearchChain
from app.chain.torrents import TorrentsChain
from app.db.oper.subscribe import SubscribeOper
from app.db.oper.systemconfig import SystemConfigOper
from app.db.oper.transferhistory import TransferHistoryOper
from app.schemas.types import EventType, MediaSource, MediaType, NotificationType, SystemConfigKey
from app.sdk.config import settings
from app.sdk.events import Event, eventmanager
from app.sdk.logging import logger
from app.sdk.media import MediaInfo, MetaInfo, normalize_media_source
from app.sdk.plugin import _PluginBase
from app.sdk.services import RuleHelper

_RE_4K = re.compile(r"(?i)(?:^|[\s._\-\[\(（【/])(2160p|4k|uhd)(?:$|[\s._\-\]\)）】/])")
_RE_LOW_RES = re.compile(
    r"(?i)(?:^|[\s._\-\[\(（【/])(1080p|1080i|720p|576p|480p)(?:$|[\s._\-\]\)）】/])"
)
_RE_EPISODE_NUM = re.compile(r"(?i)E(\d{1,4})(?:\s*[-~]\s*E?(\d{1,4}))?")

_MAX_HISTORY_ITEMS = 100


@dataclass
class _SubView:
    """订阅记录的轻量插件视图，避免直接耦合宿主 ORM 实现。"""

    id: int
    name: str
    year: Optional[str] = None
    type: str = ""
    season: Optional[int] = None
    media_source: Optional[MediaSource] = None
    media_id: Optional[str] = None
    keyword: Optional[str] = None
    state: str = ""
    best_version: int = 0
    best_version_full: int = 0
    filter_groups: List[str] = field(default_factory=list)
    sites: Optional[List[int]] = None
    custom_words: Optional[str] = None
    episode_group: Optional[str] = None
    total_episode: Optional[int] = None
    start_episode: Optional[int] = None
    lack_episode: Optional[int] = None
    note: List[int] = field(default_factory=list)
    episode_priority: Dict[str, int] = field(default_factory=dict)

    @classmethod
    def from_record(cls, record: Any) -> "_SubView":
        """将 SubscribeOper 返回的记录归一化为插件内部视图。"""
        raw_source = getattr(record, "media_source", None)
        media_source = normalize_media_source(raw_source) if raw_source else None
        raw_id = getattr(record, "media_id", None)
        media_id = str(raw_id).strip() if raw_id not in (None, "", 0, "0") else None

        raw_groups = getattr(record, "filter_groups", None)
        filter_groups = [str(g) for g in raw_groups if g] if isinstance(raw_groups, list) else []

        raw_note = getattr(record, "note", None)
        note: List[int] = []
        if isinstance(raw_note, list):
            for item in raw_note:
                try:
                    note.append(int(item))
                except (TypeError, ValueError):
                    continue

        raw_ep_pri = getattr(record, "episode_priority", None)
        ep_pri: Dict[str, int] = {}
        if isinstance(raw_ep_pri, dict):
            for k, v in raw_ep_pri.items():
                try:
                    ep_pri[str(int(k))] = int(v)
                except (TypeError, ValueError):
                    continue

        raw_sites = getattr(record, "sites", None)
        sites = [int(s) for s in raw_sites] if isinstance(raw_sites, list) and raw_sites else None

        return cls(
            id=int(getattr(record, "id")),
            name=str(getattr(record, "name", "") or ""),
            year=str(getattr(record, "year")) if getattr(record, "year", None) else None,
            type=str(getattr(record, "type", "") or ""),
            season=int(record.season) if getattr(record, "season", None) is not None else None,
            media_source=media_source,
            media_id=media_id,
            keyword=getattr(record, "keyword", None),
            state=str(getattr(record, "state", "") or ""),
            best_version=int(getattr(record, "best_version", 0) or 0),
            best_version_full=int(getattr(record, "best_version_full", 0) or 0),
            filter_groups=filter_groups,
            sites=sites,
            custom_words=getattr(record, "custom_words", None),
            episode_group=getattr(record, "episode_group", None),
            total_episode=getattr(record, "total_episode", None),
            start_episode=getattr(record, "start_episode", None),
            lack_episode=getattr(record, "lack_episode", None),
            note=note,
            episode_priority=ep_pri,
        )

    @property
    def is_tv(self) -> bool:
        """是否为电视剧/剧集类订阅。"""
        return self.type in (MediaType.TV.value, "电视剧", "TV")

    @property
    def is_movie(self) -> bool:
        """是否为电影类订阅。"""
        return self.type in (MediaType.MOVIE.value, "电影", "Movie")

    @property
    def display_title(self) -> str:
        """便于日志与界面展示的标题。"""
        if self.is_tv and self.season is not None:
            return f"{self.name} S{self.season:02d}"
        return self.name


def _is_4k_text(text: Optional[str]) -> bool:
    """判断文件名或标题文本是否明确包含 4K / 2160p / UHD 标识。"""
    if not text:
        return False
    return bool(_RE_4K.search(str(text)))


def _is_low_res_text(text: Optional[str]) -> bool:
    """判断文件名或标题文本是否明确为低于 4K 的分辨率。"""
    if not text:
        return False
    text_str = str(text)
    if _RE_4K.search(text_str):
        return False
    return bool(_RE_LOW_RES.search(text_str))


def _is_4k_meta(meta: Any, *fallback_texts: Optional[str]) -> bool:
    """优先依据 MetaInfo.resource_pix，再结合原始文本判断是否 4K。"""
    pix = getattr(meta, "resource_pix", None) if meta is not None else None
    if isinstance(pix, str) and pix.strip():
        pix_lower = pix.strip().lower()
        if any(tag in pix_lower for tag in ("2160", "4k", "uhd")):
            return True
        if any(tag in pix_lower for tag in ("1080", "720", "576", "480")):
            return False
    for text in fallback_texts:
        if _is_4k_text(text):
            return True
    return False


def _parse_episode_numbers(ep_str: Optional[str]) -> List[int]:
    """解析整理历史中的 episodes 字符串（如 'E01'、'E01-E03'、'E01,E02'）。"""
    if not ep_str:
        return []
    episodes: List[int] = []
    for match in _RE_EPISODE_NUM.finditer(str(ep_str)):
        start = int(match.group(1))
        end_str = match.group(2)
        if end_str:
            end = int(end_str)
            if end >= start and (end - start) <= 200:
                episodes.extend(range(start, end + 1))
            else:
                episodes.append(start)
        else:
            episodes.append(start)
    return sorted(set(episodes))


def _format_ts(ts: Optional[float] = None) -> str:
    """将时间戳格式化为可读本地时间。"""
    dt = datetime.datetime.fromtimestamp(ts if ts is not None else time.time())
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class SubscribeAuto4K(_PluginBase):
    """默认允许1080p兜底，确认出现4K后自动收紧为仅4K并洗版此前低分辨率剧集。"""

    plugin_name = "订阅自适应4K"
    plugin_desc = (
        "默认允许1080p兜底；当事件、整理历史、种子缓存或站点搜索发现4K后，"
        "自动追加仅4K规则组收紧订阅，并洗版此前的低分辨率剧集。"
    )
    plugin_icon = "teamwork.png"
    plugin_version = "2.0.5"
    plugin_author = "iErics"
    author_url = "https://github.com/iErics"
    plugin_config_prefix = "subscribeauto4k_"
    plugin_order = 27
    auth_level = 2

    LOG_TAG = "[SubscribeAuto4K] "

    # 运行状态与配置默认值（与 v1.0.0 全部配置键保持兼容）
    _enabled: bool = False
    _strict_group: str = "仅4K"
    _auto_upgrade: bool = True
    _upgrade_priority: int = 95
    _scan_cron: str = "*/30 * * * *"
    _check_ttl_hours: float = 6.0
    _batch_limit: int = 10

    # v2.0.0 新增增强配置
    _onlyonce: bool = False
    _dry_run: bool = False
    _enable_event_watch: bool = True
    _enable_active_search: bool = True
    _include_movie: bool = True
    _upgrade_mode: str = "requeue"  # "requeue" | "best_version"
    _upgrade_max_episodes: int = 20
    _exclude_groups: List[str] = []
    _exclude_keywords: str = ""
    _notify: bool = False

    _scheduler: Optional[BackgroundScheduler] = None
    _lock: Optional[threading.Lock] = None
    _event: Optional[threading.Event] = None

    def init_plugin(self, config: Optional[dict] = None) -> None:
        """读取配置、清理旧定时任务，并按需启动一次性扫描。"""
        self.stop_service()
        if self._lock is None:
            self._lock = threading.Lock()
        if self._event is None:
            self._event = threading.Event()
        else:
            self._event.clear()

        cfg = config or {}
        self._enabled = bool(cfg.get("enabled", False))
        self._strict_group = str(cfg.get("strict_group") or "仅4K").strip()
        self._auto_upgrade = bool(cfg.get("auto_upgrade", True))
        self._upgrade_priority = self._to_int(cfg.get("upgrade_priority"), 95, minimum=1, maximum=100)
        self._scan_cron = str(cfg.get("scan_cron") or "*/30 * * * *").strip()
        self._check_ttl_hours = self._to_float(cfg.get("check_ttl_hours"), 6.0, minimum=0.0)
        self._batch_limit = self._to_int(cfg.get("batch_limit"), 10, minimum=1, maximum=200)

        self._onlyonce = bool(cfg.get("onlyonce", False))
        self._dry_run = bool(cfg.get("dry_run", False))
        self._enable_event_watch = bool(cfg.get("enable_event_watch", True))
        self._enable_active_search = bool(cfg.get("enable_active_search", True))
        self._include_movie = bool(cfg.get("include_movie", True))
        mode = str(cfg.get("upgrade_mode") or "requeue").strip().lower()
        self._upgrade_mode = mode if mode in ("requeue", "best_version") else "requeue"
        self._upgrade_max_episodes = self._to_int(
            cfg.get("upgrade_max_episodes"), 20, minimum=0, maximum=500
        )
        raw_ex_groups = cfg.get("exclude_groups")
        self._exclude_groups = (
            [str(x).strip() for x in raw_ex_groups if str(x).strip()]
            if isinstance(raw_ex_groups, list)
            else []
        )
        self._exclude_keywords = str(cfg.get("exclude_keywords") or "").strip()
        self._notify = bool(cfg.get("notify", False))

        if self._onlyonce:
            logger.info(f"{self.LOG_TAG}配置启用了立即执行一次，将在 3 秒后异步启动扫描。")
            self._onlyonce = False
            self._save_current_config()
            self._scheduler = BackgroundScheduler(timezone=settings.TZ)
            self._scheduler.add_job(
                func=self.sync_all_subscribes,
                trigger="date",
                run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                + datetime.timedelta(seconds=3),
                name="订阅4K状态同步(立即执行)",
            )
            self._scheduler.start()

    def get_state(self) -> bool:
        """返回插件当前是否启用。"""
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        """注册远程命令以便在消息渠道手动触发扫描。"""
        return [
            {
                "cmd": "/auto4k_scan",
                "event": EventType.PluginAction,
                "desc": "立即执行订阅自适应4K同步扫描",
                "category": "订阅",
                "data": {"action": "subscribe_auto_4k_scan"},
            }
        ]

    def get_api(self) -> List[Dict[str, Any]]:
        """暴露插件 REST API，供查询状态、手动扫描与快照恢复。"""
        return [
            {
                "path": "/status",
                "endpoint": self.api_status,
                "methods": ["GET"],
                "summary": "查看订阅自适应4K运行统计与最近记录",
                "description": "返回最近一次扫描摘要、已收紧数量与操作历史。",
            },
            {
                "path": "/scan",
                "endpoint": self.api_scan,
                "methods": ["POST"],
                "summary": "立即触发一次订阅4K状态同步",
                "description": "可传 dry_run=true 进行试运行，传 force=true 忽略冷却期。",
            },
            {
                "path": "/restore",
                "endpoint": self.api_restore,
                "methods": ["POST"],
                "summary": "按快照恢复指定订阅在收紧前的配置",
                "description": "传入 subscribe_id 将该订阅恢复至收紧前的 filter_groups 与洗版设置。",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        """注册定时任务，保持服务 ID 'auto' 与旧版平滑兼容。"""
        if not self._enabled or not self._scan_cron:
            return []
        try:
            trigger = CronTrigger.from_crontab(self._scan_cron)
        except Exception as exc:
            logger.error(f"{self.LOG_TAG}无效的 Cron 表达式 '{self._scan_cron}': {exc}")
            return []
        return [
            {
                "id": "auto",
                "name": "订阅4K状态同步",
                "trigger": trigger,
                "func": self.sync_all_subscribes,
                "kwargs": {},
            }
        ]

    def stop_service(self) -> None:
        """停止插件内部调度器并设置退出信号。"""
        if self._event is not None:
            self._event.set()
        if self._scheduler is not None:
            try:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._scheduler.shutdown(wait=False)
            except Exception as exc:
                logger.debug(f"{self.LOG_TAG}停止临时调度器异常: {exc}")
            finally:
                self._scheduler = None

    # ------------------------------------------------------------------
    # 配置表单 & 详情页面（原生 Vuetify JSON，零前端构建依赖）
    # ------------------------------------------------------------------

    def _rule_group_names(self) -> List[str]:
        """读取系统已配置的过滤规则组名称列表。"""
        try:
            groups = RuleHelper.get_rule_groups() or []
            return [str(g.name) for g in groups if getattr(g, "name", None)]
        except Exception as exc:
            logger.debug(f"{self.LOG_TAG}读取过滤规则组列表失败: {exc}")
            return []

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        """返回插件配置表单与默认值。"""
        group_names = self._rule_group_names()
        strict_items = group_names or [self._strict_group or "仅4K"]

        form = [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VSheet",
                        "props": {
                            "rounded": "lg",
                            "class": "px-4 py-2 mb-5",
                            "style": "background: rgba(255, 255, 255, 0.04); border: 1px solid rgba(255, 255, 255, 0.08);",
                        },
                        "content": [
                            {
                                "component": "VRow",
                                "props": {"dense": True, "align": "center"},
                                "content": [
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "sm": 4},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "enabled",
                                                    "label": "启用插件",
                                                    "color": "primary",
                                                    "hideDetails": True,
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "sm": 4},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "auto_upgrade",
                                                    "label": "自动洗版旧集",
                                                    "color": "primary",
                                                    "hideDetails": True,
                                                },
                                            }
                                        ],
                                    },
                                    {
                                        "component": "VCol",
                                        "props": {"cols": 12, "sm": 4},
                                        "content": [
                                            {
                                                "component": "VSwitch",
                                                "props": {
                                                    "model": "onlyonce",
                                                    "label": "立即运行一次",
                                                    "color": "primary",
                                                    "hideDetails": True,
                                                },
                                            }
                                        ],
                                    },
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "strict_group",
                                            "label": "4K 收紧规则组",
                                            "items": strict_items,
                                            "density": "comfortable",
                                            "hideDetails": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "scan_cron",
                                            "label": "定时同步周期 (Cron)",
                                            "placeholder": "*/30 * * * *",
                                            "density": "comfortable",
                                            "hideDetails": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "props": {"show": "{{ Boolean(model.auto_upgrade) }}", "class": "mt-1"},
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "upgrade_mode",
                                            "label": "旧集洗版模式",
                                            "items": [
                                                {"title": "移出低清旧集重下 4K（推荐）", "value": "requeue"},
                                                {"title": "原生按集洗版 (best_version)", "value": "best_version"},
                                            ],
                                            "density": "comfortable",
                                            "hideDetails": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "upgrade_max_episodes",
                                            "label": "单剧累计洗版上限",
                                            "type": "number",
                                            "suffix": "集",
                                            "placeholder": "0 表示不限",
                                            "density": "comfortable",
                                            "hideDetails": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VExpansionPanels",
                        "props": {
                            "variant": "accordion",
                            "class": "mt-5",
                        },
                        "content": [
                            {
                                "component": "VExpansionPanel",
                                "props": {
                                    "value": "detect",
                                    "style": "background: rgba(255, 255, 255, 0.03);",
                                },
                                "content": [
                                    {
                                        "component": "VExpansionPanelTitle",
                                        "props": {"class": "text-body-2 font-weight-medium"},
                                        "text": "探测与主动搜站",
                                    },
                                    {
                                        "component": "VExpansionPanelText",
                                        "content": [
                                            {
                                                "component": "VRow",
                                                "props": {"dense": True},
                                                "content": [
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "sm": 4},
                                                        "content": [
                                                            {
                                                                "component": "VSwitch",
                                                                "props": {
                                                                    "model": "enable_event_watch",
                                                                    "label": "监听实时 4K 事件",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "sm": 4},
                                                        "content": [
                                                            {
                                                                "component": "VSwitch",
                                                                "props": {
                                                                    "model": "enable_active_search",
                                                                    "label": "允许主动搜站",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "sm": 4},
                                                        "content": [
                                                            {
                                                                "component": "VSwitch",
                                                                "props": {
                                                                    "model": "include_movie",
                                                                    "label": "包含电影订阅",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                ],
                                            },
                                            {
                                                "component": "VRow",
                                                "props": {
                                                    "show": "{{ Boolean(model.enable_active_search) }}",
                                                    "class": "mt-2",
                                                },
                                                "content": [
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "md": 6},
                                                        "content": [
                                                            {
                                                                "component": "VTextField",
                                                                "props": {
                                                                    "model": "check_ttl_hours",
                                                                    "label": "搜站冷却时间",
                                                                    "type": "number",
                                                                    "suffix": "小时",
                                                                    "density": "comfortable",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "md": 6},
                                                        "content": [
                                                            {
                                                                "component": "VTextField",
                                                                "props": {
                                                                    "model": "batch_limit",
                                                                    "label": "单轮搜站上限",
                                                                    "type": "number",
                                                                    "suffix": "条",
                                                                    "density": "comfortable",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                            {
                                "component": "VExpansionPanel",
                                "props": {
                                    "value": "advanced",
                                    "style": "background: rgba(255, 255, 255, 0.03);",
                                },
                                "content": [
                                    {
                                        "component": "VExpansionPanelTitle",
                                        "props": {"class": "text-body-2 font-weight-medium"},
                                        "text": "排除过滤与高级选项",
                                    },
                                    {
                                        "component": "VExpansionPanelText",
                                        "content": [
                                            {
                                                "component": "VRow",
                                                "props": {"dense": True},
                                                "content": [
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "sm": 6},
                                                        "content": [
                                                            {
                                                                "component": "VSwitch",
                                                                "props": {
                                                                    "model": "dry_run",
                                                                    "label": "试运行（仅检测不修改）",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "sm": 6},
                                                        "content": [
                                                            {
                                                                "component": "VSwitch",
                                                                "props": {
                                                                    "model": "notify",
                                                                    "label": "变更时发送通知",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                ],
                                            },
                                            {
                                                "component": "VRow",
                                                "props": {"class": "mt-2"},
                                                "content": [
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "md": 6},
                                                        "content": [
                                                            {
                                                                "component": "VTextField",
                                                                "props": {
                                                                    "model": "upgrade_priority",
                                                                    "label": "洗版优先级阈值 (1-100)",
                                                                    "type": "number",
                                                                    "placeholder": "默认 95",
                                                                    "density": "comfortable",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12, "md": 6},
                                                        "content": [
                                                            {
                                                                "component": "VTextField",
                                                                "props": {
                                                                    "model": "exclude_keywords",
                                                                    "label": "排除标题关键词",
                                                                    "placeholder": "逗号分隔，如：短剧, 预告片",
                                                                    "clearable": True,
                                                                    "density": "comfortable",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    },
                                                ],
                                            },
                                            {
                                                "component": "VRow",
                                                "props": {"class": "mt-2"},
                                                "content": [
                                                    {
                                                        "component": "VCol",
                                                        "props": {"cols": 12},
                                                        "content": [
                                                            {
                                                                "component": "VSelect",
                                                                "props": {
                                                                    "model": "exclude_groups",
                                                                    "label": "排除规则组",
                                                                    "items": group_names,
                                                                    "multiple": True,
                                                                    "chips": True,
                                                                    "closableChips": True,
                                                                    "clearable": True,
                                                                    "density": "comfortable",
                                                                    "hideDetails": True,
                                                                },
                                                            }
                                                        ],
                                                    }
                                                ],
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        ]

        defaults = {
            "enabled": False,
            "strict_group": "仅4K",
            "auto_upgrade": True,
            "upgrade_priority": 95,
            "scan_cron": "*/30 * * * *",
            "check_ttl_hours": 6,
            "batch_limit": 10,
            "onlyonce": False,
            "dry_run": False,
            "enable_event_watch": True,
            "enable_active_search": True,
            "include_movie": True,
            "upgrade_mode": "requeue",
            "upgrade_max_episodes": 20,
            "exclude_groups": [],
            "exclude_keywords": "",
            "notify": False,
        }
        return form, defaults

    @staticmethod
    def _format_episode_summary(eps: List[int]) -> str:
        """将剧集列表压缩为易读区间文本（如 'E01-E05, E09'），避免表格被长数组撑乱。"""
        nums = sorted({int(e) for e in eps if str(e).isdigit()})
        if not nums:
            return "-"
        ranges: List[str] = []
        start = prev = nums[0]
        for cur in nums[1:]:
            if cur == prev + 1:
                prev = cur
            else:
                ranges.append(f"E{start:02d}" if start == prev else f"E{start:02d}-E{prev:02d}")
                start = prev = cur
        ranges.append(f"E{start:02d}" if start == prev else f"E{start:02d}-E{prev:02d}")
        joined = ", ".join(ranges)
        if len(ranges) > 4:
            return f"{len(nums)}集 ({', '.join(ranges[:3])}…)"
        return joined

    def get_page(self) -> List[dict]:
        """渲染卡片化状态看板与精简处理明细表。"""
        checked = self._load_checked()
        history = self._load_history()
        last_run = self.get_data("last_run") or {}
        snapshots = self._load_snapshots()

        tightened_count = sum(1 for item in checked.values() if isinstance(item, dict) and item.get("tightened"))
        total_checked = len(checked)
        last_time = str(last_run.get("time") or "尚未运行")
        if len(last_time) >= 16:
            last_time = last_time[5:16]

        source_labels = {
            "transfer_history": "整理历史",
            "torrent_cache": "种子缓存",
            "active_search": "主动搜站",
            "event:transfer": "实时整理",
            "event:download": "实时下载",
            "already_strict": "已收紧补检",
            "api:restore": "快照恢复",
        }

        def _action_color(action_text: str) -> str:
            if "试运行" in action_text:
                return "warning"
            if "恢复" in action_text or "回滚" in action_text:
                return "secondary"
            if "补洗版" in action_text:
                return "info"
            return "success"

        rows = []
        for entry in history[:30]:
            eps = entry.get("upgraded_episodes") or []
            eps_text = self._format_episode_summary(eps)
            raw_time = str(entry.get("time") or "-")
            short_time = raw_time[5:16] if len(raw_time) >= 16 else raw_time
            raw_src = str(entry.get("source") or "-")
            src_label = source_labels.get(raw_src, raw_src)
            action_text = str(entry.get("action") or "-")
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {"component": "td", "props": {"class": "text-no-wrap text-caption"}, "text": short_time},
                        {"component": "td", "props": {"class": "font-weight-medium"}, "text": str(entry.get("title") or "-")},
                        {"component": "td", "props": {"class": "text-no-wrap"}, "text": src_label},
                        {
                            "component": "td",
                            "props": {"class": "text-no-wrap"},
                            "content": [
                                {
                                    "component": "VChip",
                                    "props": {
                                        "size": "x-small",
                                        "variant": "tonal",
                                        "color": _action_color(action_text),
                                    },
                                    "text": action_text,
                                }
                            ],
                        },
                        {"component": "td", "props": {"class": "text-caption"}, "text": eps_text},
                    ],
                }
            )

        if not rows:
            rows.append(
                {
                    "component": "tr",
                    "content": [
                        {
                            "component": "td",
                            "props": {"colspan": 5, "class": "text-center text-medium-emphasis py-4"},
                            "text": "暂无 4K 收紧或洗版记录",
                        }
                    ],
                }
            )

        def _metric_card(label: str, value: str, subtext: str) -> dict:
            return {
                "component": "VCol",
                "props": {"cols": 6, "md": 3},
                "content": [
                    {
                        "component": "VCard",
                        "props": {"variant": "outlined", "class": "pa-3"},
                        "content": [
                            {"component": "div", "props": {"class": "text-caption text-medium-emphasis"}, "text": label},
                            {"component": "div", "props": {"class": "text-h6 font-weight-bold mt-1"}, "text": value},
                            {"component": "div", "props": {"class": "text-caption text-medium-emphasis mt-1"}, "text": subtext},
                        ],
                    }
                ],
            }

        last_sub = (
            f"命中 {last_run.get('local_hits', 0)} · 搜站 {last_run.get('search_calls', 0)} · 洗版 {last_run.get('upgraded_episodes', 0)} 集"
            if last_run
            else "等待首次执行"
        )

        return [
            {
                "component": "VRow",
                "props": {"dense": True},
                "content": [
                    _metric_card("已跟踪订阅", f"{total_checked}", "当前纳入监控"),
                    _metric_card("已收紧仅 4K", f"{tightened_count}", f"规则组：{self._strict_group}"),
                    _metric_card("回滚快照", f"{len(snapshots)}", "支持一键恢复"),
                    _metric_card("最近扫描", last_time, last_sub),
                ],
            },
            {
                "component": "VCard",
                "props": {"variant": "outlined", "class": "pa-3 mt-3"},
                "content": [
                    {"component": "VCardTitle", "props": {"class": "text-subtitle-1 pa-0 pb-2"}, "text": "最近处理记录"},
                    {"component": "VDivider", "props": {"class": "mb-2"}},
                    {
                        "component": "VTable",
                        "props": {"hover": True, "density": "compact"},
                        "content": [
                            {
                                "component": "thead",
                                "content": [
                                    {
                                        "component": "tr",
                                        "content": [
                                            {"component": "th", "text": "时间"},
                                            {"component": "th", "text": "订阅"},
                                            {"component": "th", "text": "信号来源"},
                                            {"component": "th", "text": "动作"},
                                            {"component": "th", "text": "洗版剧集"},
                                        ],
                                    }
                                ],
                            },
                            {
                                "component": "tbody",
                                "content": rows,
                            },
                        ],
                    },
                ],
            },
        ]

    # ------------------------------------------------------------------
    # 事件监听（实时零延迟触发 + 远程命令）
    # ------------------------------------------------------------------

    @eventmanager.register(EventType.PluginAction)
    def handle_plugin_action(self, event: Event) -> None:
        """响应 /auto4k_scan 远程指令。"""
        if not event or not event.event_data:
            return
        data = event.event_data
        if data.get("action") != "subscribe_auto_4k_scan":
            return
        logger.info(f"{self.LOG_TAG}收到远程指令，开始执行订阅自适应4K同步。")
        summary = self.sync_all_subscribes(force=False)
        channel = data.get("channel")
        userid = data.get("user")
        if channel:
            self.post_message(
                channel=channel,
                title="订阅自适应4K同步完成",
                text=(
                    f"扫描订阅：{summary.get('scanned', 0)} 条\n"
                    f"本地命中：{summary.get('local_hits', 0)} 条\n"
                    f"主动搜站：{summary.get('search_calls', 0)} 条\n"
                    f"新收紧 4K：{summary.get('tightened', 0)} 条\n"
                    f"触发洗版：{summary.get('upgraded_episodes', 0)} 集"
                    + ("\n（当前为试运行模式，未写入修改）" if summary.get("dry_run") else "")
                ),
                userid=userid,
            )

    @eventmanager.register(EventType.TransferComplete)
    def on_transfer_complete(self, event: Event) -> None:
        """整理完成时，若刚入库 4K 资源，立即触发对应订阅收紧与旧集洗版。"""
        if not self._enabled or not self._enable_event_watch:
            return
        data = event.event_data if event else None
        if not isinstance(data, dict):
            return
        meta = data.get("meta")
        mediainfo = data.get("mediainfo")
        transferinfo = data.get("transferinfo")
        fileitem = data.get("fileitem")

        texts = [
            getattr(fileitem, "name", None),
            getattr(fileitem, "path", None),
            getattr(getattr(transferinfo, "target_item", None), "name", None),
            getattr(getattr(transferinfo, "fileitem", None), "name", None),
        ]
        if not _is_4k_meta(meta, *texts):
            return
        self._handle_realtime_4k_signal(mediainfo=mediainfo, meta=meta, source="event:transfer")

    @eventmanager.register(EventType.DownloadAdded)
    def on_download_added(self, event: Event) -> None:
        """添加下载任务时，若命中 4K 种子，立即触发对应订阅收紧与旧集洗版。"""
        if not self._enabled or not self._enable_event_watch:
            return
        data = event.event_data if event else None
        if not isinstance(data, dict):
            return
        context = data.get("context")
        if not context:
            return
        meta = getattr(context, "meta_info", None)
        mediainfo = getattr(context, "media_info", None)
        torrent = getattr(context, "torrent_info", None)
        texts = [
            getattr(torrent, "title", None),
            getattr(torrent, "description", None),
        ]
        if not _is_4k_meta(meta, *texts):
            return
        self._handle_realtime_4k_signal(mediainfo=mediainfo, meta=meta, source="event:download")

    # ------------------------------------------------------------------
    # API 端点
    # ------------------------------------------------------------------

    def api_status(self) -> Dict[str, Any]:
        """返回运行统计与最近记录。"""
        checked = self._load_checked()
        snapshots = self._load_snapshots()
        return {
            "enabled": self._enabled,
            "dry_run": self._dry_run,
            "strict_group": self._strict_group,
            "upgrade_mode": self._upgrade_mode,
            "tracked_count": len(checked),
            "tightened_count": sum(
                1 for item in checked.values() if isinstance(item, dict) and item.get("tightened")
            ),
            "snapshots_count": len(snapshots),
            "last_run": self.get_data("last_run") or {},
            "recent_history": self._load_history()[:20],
        }

    def api_scan(
        self,
        dry_run: Optional[bool] = None,
        force: bool = False,
        skip_search: bool = False,
        background: bool = False,
    ) -> Dict[str, Any]:
        """通过 API 触发一轮同步扫描（支持只做本地检测或后台异步运行，避免反代超时）。"""
        if background:
            t = threading.Thread(
                target=self.sync_all_subscribes,
                kwargs={
                    "dry_run_override": dry_run,
                    "force": bool(force),
                    "skip_search": bool(skip_search),
                },
                daemon=True,
                name="SubscribeAuto4K-api-scan",
            )
            t.start()
            return {"started": True, "dry_run": dry_run, "force": bool(force), "skip_search": bool(skip_search)}
        return self.sync_all_subscribes(
            dry_run_override=dry_run,
            force=bool(force),
            skip_search=bool(skip_search),
        )

    def api_restore(self, subscribe_id: int, keep_strict: bool = False) -> Dict[str, Any]:
        """
        将指定订阅恢复为收紧前保存的快照状态。
        当 keep_strict=True 时保留当前 filter_groups（继续仅下 4K），仅恢复 note / lack_episode / episode_priority。
        """
        snapshots = self._load_snapshots()
        key = str(subscribe_id)
        snap = snapshots.get(key)
        if not isinstance(snap, dict):
            return {"success": False, "message": f"未找到订阅 {subscribe_id} 的回滚快照。"}

        sub_oper = SubscribeOper()
        current = sub_oper.get(int(subscribe_id))
        if not current:
            return {"success": False, "message": f"订阅 {subscribe_id} 已不存在。"}

        payload: Dict[str, Any] = {}
        if not keep_strict:
            payload["filter_groups"] = snap.get("filter_groups") or []
            payload["best_version"] = int(snap.get("best_version", 0) or 0)
        if "note" in snap and snap["note"] is not None:
            payload["note"] = snap["note"]
        if "lack_episode" in snap and snap["lack_episode"] is not None:
            payload["lack_episode"] = snap["lack_episode"]
        if "episode_priority" in snap and snap["episode_priority"] is not None:
            payload["episode_priority"] = snap["episode_priority"]

        sub_oper.update(int(subscribe_id), payload)
        if not keep_strict:
            snapshots.pop(key, None)
            self.save_data("snapshots", snapshots)

        checked = self._load_checked()
        if key in checked and isinstance(checked[key], dict):
            if not keep_strict:
                checked[key]["tightened"] = False
                checked[key]["action"] = "restored"
            else:
                checked[key]["action"] = "restored_episodes"
            self.save_data("checked", checked)

        action_text = "已恢复集数(保留仅4K)" if keep_strict else "已回滚快照"
        self._append_history(
            {
                "time": _format_ts(),
                "subscribe_id": int(subscribe_id),
                "title": snap.get("title") or str(subscribe_id),
                "source": "api:restore",
                "action": action_text,
                "upgraded_episodes": [],
                "detail": f"keep_strict={keep_strict}, note_count={len(payload.get('note') or [])}",
            }
        )
        return {"success": True, "message": f"已恢复订阅 {subscribe_id}。", "restored": payload}

    # ------------------------------------------------------------------
    # 核心同步引擎
    # ------------------------------------------------------------------

    def sync_all_subscribes(
        self,
        *,
        dry_run_override: Optional[bool] = None,
        force: bool = False,
        skip_search: bool = False,
    ) -> Dict[str, Any]:
        """执行一轮全量订阅 4K 状态同步（预索引本地缓存 + 并发锁 + 主动搜索配额限制）。"""
        if self._lock is None:
            self._lock = threading.Lock()
        if not self._lock.acquire(blocking=False):
            logger.info(f"{self.LOG_TAG}上一轮同步尚未结束，跳过本次触发。")
            return {"skipped_locked": True}

        try:
            dry_run = self._dry_run if dry_run_override is None else bool(dry_run_override)
            allow_search = self._enable_active_search and not skip_search
            summary: Dict[str, Any] = {
                "time": _format_ts(),
                "dry_run": dry_run,
                "scanned": 0,
                "already_strict": 0,
                "local_hits": 0,
                "search_calls": 0,
                "search_hits": 0,
                "tightened": 0,
                "upgraded_episodes": 0,
                "skipped": 0,
                "errors": 0,
            }

            if not self._strict_group:
                logger.warning(f"{self.LOG_TAG}未配置 4K 收紧规则组 strict_group，跳过执行。")
                self.save_data("last_run", summary)
                return summary

            if not self._verify_strict_group_exists():
                logger.error(
                    f"{self.LOG_TAG}过滤规则组 '{self._strict_group}' 不存在，"
                    f"已中止本轮执行以防锁死订阅。请先在规则组中创建或修正插件配置。"
                )
                summary["errors"] += 1
                self.save_data("last_run", summary)
                return summary

            sub_oper = SubscribeOper()
            raw_subs = sub_oper.list(state="R") or []
            checked = self._load_checked()
            now_ts = time.time()
            ttl_seconds = max(0.0, self._check_ttl_hours) * 3600.0
            active_searches_used = 0
            torrents_chain = TorrentsChain()
            transfer_oper = TransferHistoryOper()

            # 一次性读取并预索引本地种子缓存中的所有 4K 候选，避免按订阅重复反序列化缓存
            cache_4k_index = self._build_4k_torrent_cache_index(torrents_chain)

            # 预计算本次需要跳过的关键词
            exclude_kws = self._parsed_exclude_keywords()

            # 把最久未做主动探查（search_ts 最小）的订阅排在前面，确保在 batch_limit 下公平轮询
            subs_with_record = []
            for raw_sub in raw_subs:
                try:
                    sub = _SubView.from_record(raw_sub)
                except Exception as exc:
                    logger.warning(f"{self.LOG_TAG}解析订阅记录失败: {exc}")
                    summary["errors"] += 1
                    continue
                prev = checked.get(str(sub.id)) if isinstance(checked.get(str(sub.id)), dict) else {}
                last_search_ts = float(prev.get("search_ts", 0) or 0)
                subs_with_record.append((last_search_ts, sub, prev))
            subs_with_record.sort(key=lambda item: item[0])

            # 先做全量零成本本地检测，收集待主动搜站队列
            pending_search: List[Tuple[_SubView, Dict[str, Any]]] = []
            for last_search_ts, sub, prev in subs_with_record:
                if self._event is not None and self._event.is_set():
                    logger.info(f"{self.LOG_TAG}收到停止信号，提前结束本轮扫描。")
                    break

                if self._should_skip_subscribe(sub, exclude_kws):
                    summary["skipped"] += 1
                    continue

                summary["scanned"] += 1

                already_strict = self._strict_group in sub.filter_groups
                if already_strict:
                    summary["already_strict"] += 1
                    if sub.is_tv and self._auto_upgrade:
                        upgraded = self._maybe_upgrade_existing_episodes(
                            sub=sub,
                            sub_oper=sub_oper,
                            transfer_oper=transfer_oper,
                            checked=checked,
                            source="already_strict",
                            dry_run=dry_run,
                        )
                        summary["upgraded_episodes"] += len(upgraded)
                    continue

                # 第 1 层：零成本检查整理历史是否已出现本季 4K
                has_4k, signal_source = self._detect_4k_from_transfer_history(sub, transfer_oper)

                # 第 2 层：零成本 O(1) 查预索引的本地种子缓存
                if not has_4k:
                    has_4k, signal_source = self._detect_4k_from_cache_index(sub, cache_4k_index)

                if has_4k:
                    summary["local_hits"] += 1
                    tightened, upgraded_eps = self._apply_4k_policy(
                        sub=sub,
                        sub_oper=sub_oper,
                        transfer_oper=transfer_oper,
                        checked=checked,
                        source=signal_source,
                        dry_run=dry_run,
                    )
                    if tightened:
                        summary["tightened"] += 1
                    summary["upgraded_episodes"] += len(upgraded_eps)
                elif allow_search:
                    if force or ttl_seconds <= 0 or (now_ts - last_search_ts) >= ttl_seconds:
                        pending_search.append((sub, prev))
                elif not dry_run:
                    checked[str(sub.id)] = {
                        **prev,
                        "ts": now_ts,
                        "name": sub.display_title,
                        "has_4k": False,
                        "source": "local:none",
                        "tightened": False,
                    }

            # 本地零成本阶段结束后立即落盘一次（dry_run 不污染 checked 冷却状态）
            if not dry_run:
                self.save_data("checked", checked)
            self.save_data("last_run", summary)

            # 第 3 层：受控主动搜站（严格遵守 batch_limit，仅真实搜索更新 search_ts）
            for sub, prev in pending_search:
                if self._event is not None and self._event.is_set():
                    break
                if active_searches_used >= self._batch_limit:
                    break
                active_searches_used += 1
                summary["search_calls"] += 1
                search_now = time.time()
                try:
                    has_4k, signal_source = self._detect_4k_by_active_search(sub)
                    if has_4k:
                        summary["search_hits"] += 1
                        tightened, upgraded_eps = self._apply_4k_policy(
                            sub=sub,
                            sub_oper=sub_oper,
                            transfer_oper=transfer_oper,
                            checked=checked,
                            source=signal_source,
                            dry_run=dry_run,
                        )
                        if tightened:
                            summary["tightened"] += 1
                        summary["upgraded_episodes"] += len(upgraded_eps)
                        if not dry_run and str(sub.id) in checked:
                            checked[str(sub.id)]["search_ts"] = search_now
                    elif not dry_run:
                        checked[str(sub.id)] = {
                            **prev,
                            "ts": search_now,
                            "search_ts": search_now,
                            "name": sub.display_title,
                            "has_4k": False,
                            "source": signal_source or "search:none",
                            "tightened": False,
                        }
                except Exception as exc:
                    logger.warning(f"{self.LOG_TAG}主动探查《{sub.display_title}》异常: {exc}")
                    summary["errors"] += 1
                    if not dry_run:
                        checked[str(sub.id)] = {
                            **prev,
                            "ts": search_now,
                            "search_ts": search_now,
                            "name": sub.display_title,
                            "has_4k": False,
                            "source": "search:error",
                            "tightened": False,
                        }
                if not dry_run:
                    self.save_data("checked", checked)
                self.save_data("last_run", summary)

            if not dry_run:
                self.save_data("checked", checked)
            self.save_data("last_run", summary)
            logger.info(
                f"{self.LOG_TAG}同步完成: 扫描={summary['scanned']}, "
                f"已收紧={summary['already_strict']}, 本地命中={summary['local_hits']}, "
                f"主动搜站={summary['search_calls']}(命中{summary['search_hits']}), "
                f"本轮新收紧={summary['tightened']}, 洗版集数={summary['upgraded_episodes']}"
                + (" [dry_run]" if dry_run else "")
            )
            return summary
        finally:
            self._lock.release()

    def _handle_realtime_4k_signal(
        self,
        *,
        mediainfo: Any,
        meta: Any,
        source: str,
    ) -> None:
        """收到实时 4K 下载或整理事件后，快速定位对应运行中订阅并收紧。"""
        if not mediainfo:
            return
        media_source = normalize_media_source(getattr(mediainfo, "source", None))
        media_id = getattr(mediainfo, "tmdb_id", None) or getattr(mediainfo, "douban_id", None)
        if not media_id:
            media_id = getattr(mediainfo, "media_id", None)
        media_id_str = str(media_id).strip() if media_id not in (None, "", 0, "0") else None
        season = getattr(meta, "begin_season", None) if meta is not None else None
        mtype = getattr(mediainfo, "type", None)
        mtype_val = mtype.value if hasattr(mtype, "value") else str(mtype or "")
        title = str(getattr(mediainfo, "title", "") or "")

        if self._lock is None:
            self._lock = threading.Lock()
        with self._lock:
            if not self._verify_strict_group_exists():
                return
            sub_oper = SubscribeOper()
            transfer_oper = TransferHistoryOper()
            checked = self._load_checked()
            exclude_kws = self._parsed_exclude_keywords()

            for raw_sub in sub_oper.list(state="R") or []:
                sub = _SubView.from_record(raw_sub)
                if self._should_skip_subscribe(sub, exclude_kws):
                    continue
                if not self._matches_event_identity(
                    sub=sub,
                    media_source=media_source,
                    media_id=media_id_str,
                    title=title,
                    mtype_val=mtype_val,
                    season=season,
                ):
                    continue
                if self._strict_group in sub.filter_groups:
                    continue
                self._apply_4k_policy(
                    sub=sub,
                    sub_oper=sub_oper,
                    transfer_oper=transfer_oper,
                    checked=checked,
                    source=source,
                    dry_run=self._dry_run,
                )
                self.save_data("checked", checked)

    # ------------------------------------------------------------------
    # 三层 4K 检测逻辑
    # ------------------------------------------------------------------

    def _detect_4k_from_transfer_history(
        self,
        sub: _SubView,
        transfer_oper: TransferHistoryOper,
    ) -> Tuple[bool, str]:
        """从整理历史中零成本检查该订阅当前季是否已有 4K 文件落地。"""
        records = self._query_transfer_history_for_sub(sub, transfer_oper)
        for rec in records:
            if not getattr(rec, "status", True):
                continue
            src = getattr(rec, "src", None)
            dest = getattr(rec, "dest", None)
            if _is_4k_text(dest) or _is_4k_text(src):
                return True, "transfer_history"
        return False, ""

    def _build_4k_torrent_cache_index(
        self,
        torrents_chain: TorrentsChain,
    ) -> Dict[Tuple[str, str], List[Tuple[Optional[int], Optional[int], Optional[int]]]]:
        """
        一次性扫描本地种子缓存，按规范媒体身份预索引 4K 候选：
        {(media_source_value, media_id): [(begin_season, end_season, site_id), ...]}。
        """
        index: Dict[Tuple[str, str], List[Tuple[Optional[int], Optional[int], Optional[int]]]] = {}
        try:
            cache_map = torrents_chain.get_torrents() or {}
        except Exception as exc:
            logger.debug(f"{self.LOG_TAG}读取本地种子缓存失败: {exc}")
            return index

        for contexts in cache_map.values():
            for ctx in contexts or []:
                if not ctx:
                    continue
                meta = getattr(ctx, "meta_info", None)
                torrent = getattr(ctx, "torrent_info", None)
                if not _is_4k_meta(
                    meta,
                    getattr(torrent, "title", None),
                    getattr(torrent, "description", None),
                ):
                    continue
                mediainfo = getattr(ctx, "media_info", None)
                if not mediainfo:
                    continue
                keys = self._extract_mediainfo_keys(mediainfo)
                if not keys:
                    continue
                begin_season = getattr(meta, "begin_season", None) if meta is not None else None
                end_season = (getattr(meta, "end_season", None) if meta is not None else None) or begin_season
                raw_site = getattr(torrent, "site", None) if torrent is not None else None
                site_id = int(raw_site) if raw_site is not None and str(raw_site).isdigit() else None
                for key in keys:
                    index.setdefault(key, []).append((begin_season, end_season, site_id))
        return index

    @staticmethod
    def _extract_mediainfo_keys(mediainfo: Any) -> List[Tuple[str, str]]:
        """从 MediaInfo 提取可用于精确匹配的 (media_source, media_id) 键集合。"""
        keys: List[Tuple[str, str]] = []
        tmdb_id = getattr(mediainfo, "tmdb_id", None)
        if tmdb_id not in (None, "", 0, "0"):
            keys.append((MediaSource.TMDB.value, str(tmdb_id)))
        douban_id = getattr(mediainfo, "douban_id", None)
        if douban_id not in (None, "", 0, "0"):
            keys.append((MediaSource.DOUBAN.value, str(douban_id)))
        src = normalize_media_source(getattr(mediainfo, "source", None))
        mid = getattr(mediainfo, "media_id", None)
        if src and mid not in (None, "", 0, "0"):
            pair = (src.value, str(mid))
            if pair not in keys:
                keys.append(pair)
        return keys

    @staticmethod
    def _detect_4k_from_cache_index(
        sub: _SubView,
        cache_4k_index: Dict[Tuple[str, str], List[Tuple[Optional[int], Optional[int], Optional[int]]]],
    ) -> Tuple[bool, str]:
        """O(1) 匹配预索引的本地 4K 种子缓存。"""
        if not sub.media_source or not sub.media_id:
            return False, ""
        entries = cache_4k_index.get((sub.media_source.value, str(sub.media_id)))
        if not entries:
            return False, ""
        for begin_season, end_season, site_id in entries:
            if sub.is_tv and sub.season is not None and begin_season is not None:
                if not (begin_season <= sub.season <= (end_season or begin_season)):
                    continue
            if sub.sites and site_id is not None and site_id not in sub.sites:
                continue
            return True, "torrent_cache"
        return False, ""

    def _detect_4k_by_active_search(self, sub: _SubView) -> Tuple[bool, str]:
        """使用订阅原有规则组 + strict_group 通过 SearchChain 精确探查是否存在可用 4K 资源。"""
        mediainfo = self._build_mediainfo_for_search(sub)
        if not mediainfo:
            return False, "search:unrecognized"

        strict_rule_groups = self._resolve_effective_filter_groups(sub)
        if self._strict_group not in strict_rule_groups:
            strict_rule_groups.append(self._strict_group)

        custom_words = (
            [line.strip() for line in sub.custom_words.splitlines() if line.strip()]
            if sub.custom_words
            else None
        )

        search_chain = SearchChain()
        contexts = search_chain.process(
            mediainfo=mediainfo,
            keyword=sub.keyword,
            sites=sub.sites,
            rule_groups=strict_rule_groups,
            custom_words=custom_words,
        ) or []

        for ctx in contexts:
            meta = getattr(ctx, "meta_info", None)
            torrent = getattr(ctx, "torrent_info", None)
            if sub.is_tv and sub.season is not None and meta is not None:
                begin_season = getattr(meta, "begin_season", None)
                end_season = getattr(meta, "end_season", None) or begin_season
                if begin_season is not None:
                    if not (begin_season <= sub.season <= (end_season or begin_season)):
                        continue
            if _is_4k_meta(
                meta,
                getattr(torrent, "title", None),
                getattr(torrent, "description", None),
            ):
                return True, "active_search"

        return False, "search:none"

    # ------------------------------------------------------------------
    # 规则组收紧 + 低分辨率旧集洗版
    # ------------------------------------------------------------------

    def _apply_4k_policy(
        self,
        *,
        sub: _SubView,
        sub_oper: SubscribeOper,
        transfer_oper: TransferHistoryOper,
        checked: Dict[str, Any],
        source: str,
        dry_run: bool,
    ) -> Tuple[bool, List[int]]:
        """收紧订阅为仅4K，并按需触发低分辨率旧集洗版。"""
        new_groups = self._resolve_effective_filter_groups(sub)
        if self._strict_group not in new_groups:
            new_groups.append(self._strict_group)

        upgraded_eps: List[int] = []
        payload: Dict[str, Any] = {"filter_groups": new_groups}
        prev_entry = checked.get(str(sub.id)) if isinstance(checked.get(str(sub.id)), dict) else {}
        already_upgraded: Set[int] = {
            int(e) for e in (prev_entry.get("upgraded_episodes") or []) if str(e).isdigit()
        }

        if sub.is_tv and self._auto_upgrade:
            low_res_eps = self._find_low_res_episodes(sub, transfer_oper, already_upgraded)
            if low_res_eps:
                upgraded_eps = low_res_eps
                payload.update(self._build_upgrade_payload(sub, low_res_eps))

        action_label = "试运行(将收紧4K)" if dry_run else "已收紧为仅4K"
        if upgraded_eps:
            action_label += f"+洗版{len(upgraded_eps)}集"

        detail = f"filter_groups -> {new_groups}"
        if upgraded_eps:
            detail += f"; mode={self._upgrade_mode}"

        if not dry_run:
            self._save_snapshot_if_absent(sub)
            sub_oper.update(sub.id, payload)
            logger.info(
                f"{self.LOG_TAG}《{sub.display_title}》命中 4K ({source}) -> {action_label} ({detail})"
            )
            if self._notify:
                self.post_message(
                    mtype=NotificationType.Subscribe,
                    title=f"订阅已自动收紧为 4K：{sub.display_title}",
                    text=(
                        f"信号来源：{source}\n"
                        f"规则组：{', '.join(new_groups)}\n"
                        + (
                            f"触发洗版集数：{', '.join(f'E{e:02d}' for e in upgraded_eps)} ({self._upgrade_mode})"
                            if upgraded_eps
                            else "无需洗版旧集"
                        )
                    ),
                )
        else:
            logger.info(
                f"{self.LOG_TAG}[试运行] 《{sub.display_title}》命中 4K ({source}) -> {action_label} ({detail})"
            )

        merged_upgraded = sorted(already_upgraded | set(upgraded_eps)) if not dry_run else sorted(already_upgraded)
        checked[str(sub.id)] = {
            "ts": time.time(),
            "name": sub.display_title,
            "season": sub.season,
            "has_4k": True,
            "source": source,
            "action": action_label,
            "tightened": not dry_run,
            "dry_run": dry_run,
            "upgraded_episodes": merged_upgraded,
        }
        self._append_history(
            {
                "time": _format_ts(),
                "subscribe_id": sub.id,
                "title": sub.display_title,
                "source": source,
                "action": action_label,
                "upgraded_episodes": upgraded_eps,
                "detail": detail,
            }
        )
        return True, upgraded_eps

    def _maybe_upgrade_existing_episodes(
        self,
        *,
        sub: _SubView,
        sub_oper: SubscribeOper,
        transfer_oper: TransferHistoryOper,
        checked: Dict[str, Any],
        source: str,
        dry_run: bool,
    ) -> List[int]:
        """对已收紧为仅4K但仍有未洗版低分辨率旧集的电视剧补发一次洗版。"""
        prev_entry = checked.get(str(sub.id)) if isinstance(checked.get(str(sub.id)), dict) else {}
        already_upgraded: Set[int] = {
            int(e) for e in (prev_entry.get("upgraded_episodes") or []) if str(e).isdigit()
        }
        low_res_eps = self._find_low_res_episodes(sub, transfer_oper, already_upgraded)
        if not low_res_eps:
            return []

        payload = self._build_upgrade_payload(sub, low_res_eps)
        action_label = f"试运行(补洗版{len(low_res_eps)}集)" if dry_run else f"补洗版{len(low_res_eps)}集"
        if not dry_run:
            self._save_snapshot_if_absent(sub)
            sub_oper.update(sub.id, payload)
            checked[str(sub.id)] = {
                **prev_entry,
                "ts": time.time(),
                "name": sub.display_title,
                "has_4k": True,
                "tightened": True,
                "upgraded_episodes": sorted(already_upgraded | set(low_res_eps)),
            }
            logger.info(
                f"{self.LOG_TAG}《{sub.display_title}》{action_label}: "
                f"{[f'E{e:02d}' for e in low_res_eps]} (mode={self._upgrade_mode})"
            )

        self._append_history(
            {
                "time": _format_ts(),
                "subscribe_id": sub.id,
                "title": sub.display_title,
                "source": source,
                "action": action_label,
                "upgraded_episodes": low_res_eps,
                "detail": f"mode={self._upgrade_mode}",
            }
        )
        return low_res_eps

    def _find_low_res_episodes(
        self,
        sub: _SubView,
        transfer_oper: TransferHistoryOper,
        already_upgraded: Set[int],
    ) -> List[int]:
        """结合整理历史分辨率与 episode_priority 识别需要升级为 4K 的已下载剧集。"""
        if not sub.note and not sub.episode_priority:
            return []

        # 统计每集在整理历史中是否已拥有 4K 版本、以及是否出现过非 4K 版本
        has_4k_ep: Set[int] = set()
        has_low_ep: Set[int] = set()
        for rec in self._query_transfer_history_for_sub(sub, transfer_oper):
            if not getattr(rec, "status", True):
                continue
            eps = _parse_episode_numbers(getattr(rec, "episodes", None))
            if not eps:
                continue
            src = getattr(rec, "src", None)
            dest = getattr(rec, "dest", None)
            if _is_4k_text(dest) or _is_4k_text(src):
                has_4k_ep.update(eps)
            elif _is_low_res_text(dest) or _is_low_res_text(src):
                has_low_ep.update(eps)

        candidate_eps = set(sub.note) | {int(k) for k in sub.episode_priority.keys() if k.isdigit()}
        targets: List[int] = []
        for ep in sorted(candidate_eps):
            if ep in already_upgraded or ep in has_4k_ep:
                continue
            if ep in has_low_ep:
                targets.append(ep)
                continue
            pri = sub.episode_priority.get(str(ep))
            if pri is not None and pri < self._upgrade_priority:
                targets.append(ep)

        if self._upgrade_max_episodes > 0:
            remaining_quota = max(0, self._upgrade_max_episodes - len(already_upgraded))
            if remaining_quota == 0:
                return []
            if len(targets) > remaining_quota:
                # 优先洗版最新（集号最大）的 N 集，并按单剧累计配额封顶，防止长篇年番每轮持续往前挖旧集
                targets = targets[-remaining_quota:]
        return targets

    def _build_upgrade_payload(self, sub: _SubView, low_res_eps: List[int]) -> Dict[str, Any]:
        """按所选洗版模式构造 SubscribeOper.update 的增量字段。"""
        low_set = set(low_res_eps)
        if self._upgrade_mode == "best_version":
            new_pri = dict(sub.episode_priority)
            for ep in low_res_eps:
                # 设为 1 确保任何 4K 命中优先级都严格高于当前值
                new_pri[str(ep)] = min(int(new_pri.get(str(ep), 1)), 1)
            return {
                "best_version": 1,
                "best_version_full": 0,
                "episode_priority": new_pri,
            }

        # 默认 requeue 模式：把低分辨率集从已完成列表 note 中移出，让订阅重新按仅4K补齐
        remaining_note = [ep for ep in sub.note if ep not in low_set]
        removed_count = len(sub.note) - len(remaining_note)
        new_pri = {k: v for k, v in sub.episode_priority.items() if not (k.isdigit() and int(k) in low_set)}
        payload: Dict[str, Any] = {
            "note": remaining_note,
            "episode_priority": new_pri,
        }
        if sub.total_episode is not None:
            start_ep = int(sub.start_episode or 1)
            expected_total = max(0, int(sub.total_episode) - start_ep + 1)
            payload["lack_episode"] = max(0, expected_total - len(remaining_note))
        elif sub.lack_episode is not None:
            payload["lack_episode"] = int(sub.lack_episode) + removed_count
        return payload

    # ------------------------------------------------------------------
    # 辅助方法
    # ------------------------------------------------------------------

    def _resolve_effective_filter_groups(self, sub: _SubView) -> List[str]:
        """
        返回订阅当前实际生效的规则组列表。
        若订阅自身 filter_groups 为空，先继承系统默认 SubscribeFilterRuleGroups（如「免费」「前置过滤」），
        避免追加「仅4K」后覆盖掉全局默认过滤规则。
        """
        if sub.filter_groups:
            return list(sub.filter_groups)
        try:
            defaults = SystemConfigOper().get(SystemConfigKey.SubscribeFilterRuleGroups)
            if isinstance(defaults, list):
                return [str(g) for g in defaults if g]
        except Exception as exc:
            logger.debug(f"{self.LOG_TAG}读取默认订阅过滤规则组失败: {exc}")
        return []

    def _verify_strict_group_exists(self) -> bool:
        """确认配置的 4K 规则组在系统中真实存在。"""
        try:
            group = RuleHelper().get_rule_group(self._strict_group)
            return group is not None
        except Exception as exc:
            logger.warning(f"{self.LOG_TAG}校验过滤规则组 '{self._strict_group}' 失败: {exc}")
            return False

    def _should_skip_subscribe(self, sub: _SubView, exclude_kws: List[str]) -> bool:
        """判断某条订阅是否不在处理范围内。"""
        if not sub.is_tv and not (self._include_movie and sub.is_movie):
            return True
        if self._exclude_groups and any(g in self._exclude_groups for g in sub.filter_groups):
            return True
        if exclude_kws and any(kw.lower() in sub.name.lower() for kw in exclude_kws):
            return True
        return False

    def _parsed_exclude_keywords(self) -> List[str]:
        """解析按换行或逗号分隔的排除关键词列表。"""
        if not self._exclude_keywords:
            return []
        parts = re.split(r"[\n,，]+", self._exclude_keywords)
        return [p.strip() for p in parts if p.strip()]

    def _matches_event_identity(
        self,
        *,
        sub: _SubView,
        media_source: Optional[MediaSource],
        media_id: Optional[str],
        title: str,
        mtype_val: str,
        season: Optional[int],
    ) -> bool:
        """比对事件媒体身份与订阅身份是否一致。"""
        if sub.is_tv and season is not None and sub.season is not None and int(season) != int(sub.season):
            return False
        if media_source and media_id and sub.media_source and sub.media_id:
            if sub.media_source == media_source and str(sub.media_id) == str(media_id):
                return True
            return False
        if title and sub.name and title.strip().lower() == sub.name.strip().lower():
            if not mtype_val or mtype_val == sub.type:
                return True
        return False

    def _query_transfer_history_for_sub(
        self,
        sub: _SubView,
        transfer_oper: TransferHistoryOper,
    ) -> List[Any]:
        """按 V3 规范媒体身份查询该订阅对应的整理历史记录。"""
        season_str = f"S{sub.season:02d}" if (sub.is_tv and sub.season is not None) else None
        try:
            records = transfer_oper.get_by(
                title=sub.name,
                year=sub.year,
                mtype=sub.type,
                season=season_str,
                media_source=sub.media_source,
                media_id=sub.media_id,
                match_all_versions=True,
            )
            return list(records or [])
        except Exception as exc:
            logger.debug(f"{self.LOG_TAG}查询《{sub.display_title}》整理历史失败: {exc}")
            return []

    def _build_mediainfo_for_search(self, sub: _SubView) -> Optional[MediaInfo]:
        """优先调用 MediaChain 识别完整 MediaInfo，失败时构建最小可用 MediaInfo。"""
        mtype = MediaType.TV if sub.is_tv else MediaType.MOVIE
        meta = MetaInfo(sub.name)
        meta.type = mtype
        if sub.year:
            meta.year = sub.year
        if sub.is_tv and sub.season is not None:
            meta.begin_season = sub.season

        try:
            media_chain = MediaChain()
            recognize = media_chain.recognize_media
            params = inspect.signature(recognize).parameters
            kwargs: Dict[str, Any] = {"meta": meta, "mtype": mtype}
            if "episode_group" in params and sub.episode_group:
                kwargs["episode_group"] = sub.episode_group
            if "cache" in params:
                kwargs["cache"] = True

            if sub.media_source and sub.media_id:
                if "media_source" in params and "media_id" in params:
                    kwargs["media_source"] = sub.media_source
                    kwargs["media_id"] = sub.media_id
                elif sub.media_source == MediaSource.TMDB and "tmdbid" in params and sub.media_id.isdigit():
                    kwargs["tmdbid"] = int(sub.media_id)
                elif sub.media_source == MediaSource.DOUBAN and "doubanid" in params:
                    kwargs["doubanid"] = sub.media_id

            recognized = recognize(**kwargs)
            if recognized:
                return recognized
        except Exception as exc:
            logger.debug(f"{self.LOG_TAG}MediaChain 识别《{sub.display_title}》失败，尝试降级构造: {exc}")

        try:
            info = MediaInfo()
            info.type = mtype
            info.title = sub.name
            info.year = sub.year
            if sub.media_source == MediaSource.TMDB and sub.media_id and sub.media_id.isdigit():
                info.tmdb_id = int(sub.media_id)
            elif sub.media_source == MediaSource.DOUBAN and sub.media_id:
                info.douban_id = sub.media_id
            return info
        except Exception:
            return None

    def _save_snapshot_if_absent(self, sub: _SubView) -> None:
        """首次修改某条订阅前保存原始配置快照，便于回滚。"""
        snapshots = self._load_snapshots()
        key = str(sub.id)
        if key in snapshots:
            return
        snapshots[key] = {
            "saved_at": _format_ts(),
            "title": sub.display_title,
            "filter_groups": list(sub.filter_groups),
            "best_version": sub.best_version,
            "note": list(sub.note),
            "lack_episode": sub.lack_episode,
            "episode_priority": dict(sub.episode_priority),
        }
        self.save_data("snapshots", snapshots)

    def _load_checked(self) -> Dict[str, Any]:
        """读取持久化检查状态映射。"""
        data = self.get_data("checked")
        return dict(data) if isinstance(data, dict) else {}

    def _load_snapshots(self) -> Dict[str, Any]:
        """读取持久化回滚快照映射。"""
        data = self.get_data("snapshots")
        return dict(data) if isinstance(data, dict) else {}

    def _load_history(self) -> List[Dict[str, Any]]:
        """读取最近动作历史列表。"""
        data = self.get_data("history")
        return list(data) if isinstance(data, list) else []

    def _append_history(self, entry: Dict[str, Any]) -> None:
        """前置插入最新操作记录并截断至上限。"""
        history = self._load_history()
        history.insert(0, entry)
        self.save_data("history", history[:_MAX_HISTORY_ITEMS])

    def _save_current_config(self) -> None:
        """将当前内存配置写回插件存储。"""
        self.update_config(
            {
                "enabled": self._enabled,
                "strict_group": self._strict_group,
                "auto_upgrade": self._auto_upgrade,
                "upgrade_priority": self._upgrade_priority,
                "scan_cron": self._scan_cron,
                "check_ttl_hours": self._check_ttl_hours,
                "batch_limit": self._batch_limit,
                "onlyonce": self._onlyonce,
                "dry_run": self._dry_run,
                "enable_event_watch": self._enable_event_watch,
                "enable_active_search": self._enable_active_search,
                "include_movie": self._include_movie,
                "upgrade_mode": self._upgrade_mode,
                "upgrade_max_episodes": self._upgrade_max_episodes,
                "exclude_groups": self._exclude_groups,
                "exclude_keywords": self._exclude_keywords,
                "notify": self._notify,
            }
        )

    @staticmethod
    def _to_int(value: Any, default: int, *, minimum: int, maximum: int) -> int:
        """安全解析整型配置并限制边界。"""
        try:
            num = int(value)
        except (TypeError, ValueError):
            return default
        return max(minimum, min(maximum, num))

    @staticmethod
    def _to_float(value: Any, default: float, *, minimum: float) -> float:
        """安全解析浮点配置并限制下限。"""
        try:
            num = float(value)
        except (TypeError, ValueError):
            return default
        return max(minimum, num)
