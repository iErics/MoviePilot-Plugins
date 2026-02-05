import datetime
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

from app.chain.download import DownloadChain
from app.chain.search import SearchChain
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfo
from app.helper.mediaserver import MediaServerHelper
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
    plugin_version = "1.0"
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
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "accelerate_interval_minutes",
                                            "label": "加速更新间隔（分钟）",
                                            "type": "number",
                                            "min": 1
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "backfill_interval_hours",
                                            "label": "补全缺失间隔（小时）",
                                            "type": "number",
                                            "min": 1
                                        }
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "resume_limit",
                                            "label": "继续观看读取数量",
                                            "type": "number",
                                            "min": 1
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "resume_days",
                                            "label": "最近观看天数范围",
                                            "type": "number",
                                            "min": 1
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "user_whitelist",
                                            "label": "用户白名单（逗号分隔）"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "user_blacklist",
                                            "label": "用户黑名单（逗号分隔）"
                                        }
                                    }
                                ]
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 8},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "仅处理Emby继续观看中的电视剧；更新中无缺失时走加速更新，有缺失或已完结走补全策略。"
                                        }
                                    }
                                ]
                            }
                        ]
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
            "user_blacklist": ""
        }

    def get_page(self) -> Optional[List[dict]]:
        stats = self.get_data("last_stats") or {}
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
            }
        ]

    def stop_service(self):
        pass

    def _run_accelerate(self):
        self._process(mode="accelerate")

    def _run_backfill(self):
        self._process(mode="backfill")

    def _process(self, mode: str):
        if not self._enabled:
            return
        if not _lock.acquire(blocking=False):
            logger.info("继续观看加速任务正在运行，跳过本次执行")
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
            "skipped_non_tv": 0,
            "skipped_no_mediainfo": 0,
            "skipped_no_seriesinfo": 0
        }
        try:
            logger.info(f"继续观看加速任务开始，模式：{mode}")
            services = MediaServerHelper().get_services()
            if not services:
                logger.info("未检测到媒体服务器配置，任务结束")
                return
            for name, service in services.items():
                if not service or service.type != "emby" or not service.instance:
                    continue
                logger.info(f"开始处理Emby服务器：{name}")
                stats["servers"] += 1
                self._process_emby_service(service.instance, mode=mode, stats=stats)
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
                f"跳过非电视剧：{stats['skipped_non_tv']}，"
                f"跳过识别失败：{stats['skipped_no_mediainfo']}，"
                f"跳过详情失败：{stats['skipped_no_seriesinfo']}"
            )
            _lock.release()

    def _process_emby_service(self, emby, mode: str, stats: Dict[str, int]):
        resume_items = self._get_resume_items(emby)
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
            is_ended = self._is_ended(mediainfo)
            status_text = "已完结" if is_ended else "更新中"

            if is_ended:
                if no_exists:
                    logger.info(f"{mediainfo.title_year} 状态：{status_text}，执行补全")
                    stats["backfill_attempts"] += 1
                    if self._backfill_series(search_chain, download_chain, mediainfo, meta, no_exists):
                        stats["backfill_downloads"] += 1
                continue

            if no_exists:
                logger.info(f"{mediainfo.title_year} 状态：{status_text}，存在缺失集，执行补全")
                stats["backfill_attempts"] += 1
                if self._backfill_series(search_chain, download_chain, mediainfo, meta, no_exists):
                    stats["backfill_downloads"] += 1
                continue

            if mode == "accelerate":
                logger.info(f"{mediainfo.title_year} 状态：{status_text}，执行加速更新")
                stats["accelerate_attempts"] += 1
                if self._accelerate_series(search_chain, download_chain, mediainfo, meta, current_season):
                    stats["accelerate_downloads"] += 1

    def _get_resume_items(self, emby) -> List[dict]:
        users = self._get_emby_users(emby)
        if not users:
            logger.info("用户列表为空，无法获取继续观看")
            return []
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
                   "&Fields=ProviderIds,SeriesId,ParentIndexNumber,IndexNumber,ProductionYear,Path")
            res = emby.get_data(url)
            if not res or res.status_code != 200:
                logger.error(f"获取Emby继续观看数据失败：{user.get('Name') or user_id}")
                continue
            items = res.json().get("Items") or []
            episode_items = [item for item in items if item.get("Type") == "Episode"]
            logger.info(f"用户 {user.get('Name') or user_id} 继续观看剧集数：{len(episode_items)}")
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

    def _merge_resume_series(self, items: List[dict]) -> List[dict]:
        series_map: Dict[str, dict] = {}
        now = datetime.datetime.now()
        for item in items:
            series_id = item.get("SeriesId")
            season = item.get("ParentIndexNumber")
            episode = item.get("IndexNumber")
            if not series_id or not season:
                continue
            key = f"{series_id}:{season}"
            last_played = item.get("UserData", {}).get("LastPlayedDate")
            if last_played and "." in last_played:
                last_played = last_played.split(".")[0]
            last_played_dt = None
            if last_played:
                try:
                    last_played_dt = datetime.datetime.strptime(last_played, "%Y-%m-%dT%H:%M:%S")
                except Exception:
                    last_played_dt = None
            if self._resume_days and last_played_dt:
                if (now - last_played_dt).days > self._resume_days:
                    continue
            elif self._resume_days and not last_played_dt:
                continue
            record = series_map.get(key)
            if not record or (last_played_dt and last_played_dt > record.get("last_played", datetime.datetime.min)):
                series_map[key] = {
                    "series_id": series_id,
                    "season": int(season) if season else None,
                    "episode": int(episode) if episode else None,
                    "last_played": last_played_dt or datetime.datetime.min
                }
        logger.info(f"继续观看去重后剧集数：{len(series_map)}")
        return list(series_map.values())

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
        status = (mediainfo.status or "").lower()
        return status in ["ended", "canceled", "cancelled"]

    def _accelerate_series(self, search_chain: SearchChain, download_chain: DownloadChain,
                           mediainfo: MediaInfo, meta: MetaInfo, season: int) -> bool:
        mediakey = mediainfo.tmdb_id or mediainfo.douban_id
        if not mediakey:
            return False
        no_exists = {
            mediakey: {
                season: NotExistMediaInfo(season=season, episodes=[])
            }
        }
        contexts = search_chain.process(mediainfo=mediainfo, no_exists=no_exists)
        logger.info(f"{mediainfo.title_year} 加速搜索命中资源数：{len(contexts)}")
        if not contexts:
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
