"""Unit tests for SubscribeAuto4K v2.0.0 with mocked MoviePilot V3 SDK & Opers."""

from __future__ import annotations

import importlib.util
import sys
import types
import unittest
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional


def _install_mock_moviepilot_sdk() -> Dict[str, Any]:
    """Install minimal MoviePilot V3 SDK stubs into sys.modules for offline unit testing."""
    state: Dict[str, Any] = {
        "rule_groups": {"仅4K": object(), "免费": object(), "日番": object(), "综艺": object()},
        "subscribes": [],
        "updated_calls": [],
        "transfer_records": [],
        "cache_candidates": [],
        "search_results": [],
    }

    class EventType(Enum):
        PluginAction = "plugin.action"
        TransferComplete = "transfer.complete"
        DownloadAdded = "download.added"

    class MediaType(Enum):
        MOVIE = "电影"
        TV = "电视剧"

    class MediaSource(Enum):
        TMDB = "themoviedb"
        Douban = "douban"

    class NotificationType(Enum):
        Subscribe = "订阅"

    class SystemConfigKey(Enum):
        SubscribeFilterRuleGroups = "SubscribeFilterRuleGroups"

    class SystemConfigOper:
        def get(self, key: Any) -> Any:
            if key == SystemConfigKey.SubscribeFilterRuleGroups:
                return ["免费", "前置过滤"]
            return None

    class Event:
        def __init__(self, event_data: dict | None = None):
            self.event_data = event_data or {}

    class _EventManager:
        def register(self, _etype: Any):
            def deco(fn: Any) -> Any:
                return fn

            return deco

    class _Logger:
        def info(self, *_a: Any, **_kw: Any) -> None:
            pass

        def warning(self, *_a: Any, **_kw: Any) -> None:
            pass

        def error(self, *_a: Any, **_kw: Any) -> None:
            pass

        def debug(self, *_a: Any, **_kw: Any) -> None:
            pass

    class _PluginBase:
        def __init__(self) -> None:
            self._store: Dict[str, Any] = {}
            self._config: Dict[str, Any] = {}
            self._messages: List[Dict[str, Any]] = []

        def get_data(self, key: str) -> Any:
            return self._store.get(key)

        def save_data(self, key: str, value: Any) -> None:
            self._store[key] = value

        def update_config(self, cfg: dict) -> bool:
            self._config = dict(cfg)
            return True

        def post_message(self, **kwargs: Any) -> None:
            self._messages.append(kwargs)

    class MetaInfo:
        def __init__(self, title: str = "", resource_pix: str | None = None, begin_season: int | None = None):
            self.title = title
            self.resource_pix = resource_pix
            self.begin_season = begin_season
            self.end_season = begin_season
            self.type = MediaType.TV
            self.year = None

    class MediaInfo:
        def __init__(self) -> None:
            self.type = MediaType.TV
            self.title = ""
            self.year = None
            self.source = MediaSource.TMDB
            self.tmdb_id = None
            self.douban_id = None

    def normalize_media_source(val: Any) -> Optional[MediaSource]:
        if isinstance(val, MediaSource):
            return val
        if str(val or "").lower() in ("themoviedb", "tmdb"):
            return MediaSource.TMDB
        if str(val or "").lower() == "douban":
            return MediaSource.Douban
        return None

    class RuleHelper:
        @staticmethod
        def get_rule_groups() -> List[Any]:
            return [types.SimpleNamespace(name=n) for n in state["rule_groups"]]

        def get_rule_group(self, name: str) -> Any:
            return state["rule_groups"].get(name)

    class SubscribeOper:
        def list(self, state: Optional[str] = None) -> List[Any]:
            return [s for s in _state_ref["subscribes"] if state is None or s.state == state]

        def get(self, sid: int) -> Any:
            for s in _state_ref["subscribes"]:
                if s.id == sid:
                    return s
            return None

        def update(self, sid: int, payload: Dict[str, Any]) -> Any:
            _state_ref["updated_calls"].append((sid, payload))
            sub = self.get(sid)
            if sub:
                for k, v in payload.items():
                    setattr(sub, k, v)
            return sub

    _state_ref = state

    class TransferHistoryOper:
        def get_by(self, **_kwargs: Any) -> List[Any]:
            return list(state["transfer_records"])

    class TorrentsChain:
        def get_torrents(self, *_args: Any, **_kwargs: Any) -> Dict[str, List[Any]]:
            return {"mock.site": list(state["cache_candidates"])}

    class SearchChain:
        def process(self, **_kwargs: Any) -> List[Any]:
            return list(state["search_results"])

    class MediaChain:
        def recognize_media(self, **kwargs: Any) -> MediaInfo:
            info = MediaInfo()
            info.title = kwargs["meta"].title
            info.type = kwargs["mtype"]
            return info

    # Minimal apscheduler & pytz stubs so test runs with zero external dependencies
    class _FakeCron:
        @staticmethod
        def from_crontab(expr: str) -> str:
            if len(expr.split()) != 5:
                raise ValueError("bad cron")
            return f"cron({expr})"

    class _FakeScheduler:
        def __init__(self, timezone: Any = None) -> None:
            self.running = False
            self.jobs: List[Any] = []

        def add_job(self, **kwargs: Any) -> None:
            self.jobs.append(kwargs)

        def start(self) -> None:
            self.running = True

        def remove_all_jobs(self) -> None:
            self.jobs.clear()

        def shutdown(self, wait: bool = False) -> None:
            self.running = False

    def _mk(name: str, **attrs: Any) -> types.ModuleType:
        mod = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[name] = mod
        return mod

    _mk("pytz", timezone=lambda _tz: None)
    _mk("apscheduler")
    _mk("apscheduler.schedulers")
    _mk("apscheduler.schedulers.background", BackgroundScheduler=_FakeScheduler)
    _mk("apscheduler.triggers")
    _mk("apscheduler.triggers.cron", CronTrigger=_FakeCron)

    _mk("app")
    _mk("app.schemas")
    _mk(
        "app.schemas.types",
        EventType=EventType,
        MediaSource=MediaSource,
        MediaType=MediaType,
        NotificationType=NotificationType,
        SystemConfigKey=SystemConfigKey,
    )
    _mk("app.sdk")
    _mk("app.sdk.config", settings=types.SimpleNamespace(TZ="Asia/Shanghai"))
    _mk("app.sdk.events", Event=Event, eventmanager=_EventManager())
    _mk("app.sdk.logging", logger=_Logger())
    _mk(
        "app.sdk.media",
        MediaInfo=MediaInfo,
        MetaInfo=MetaInfo,
        normalize_media_source=normalize_media_source,
    )
    _mk("app.sdk.plugin", _PluginBase=_PluginBase)
    _mk("app.sdk.services", RuleHelper=RuleHelper)
    _mk("app.db")
    _mk("app.db.oper")
    _mk("app.db.oper.subscribe", SubscribeOper=SubscribeOper)
    _mk("app.db.oper.systemconfig", SystemConfigOper=SystemConfigOper)
    _mk("app.db.oper.transferhistory", TransferHistoryOper=TransferHistoryOper)
    _mk("app.chain")
    _mk("app.chain.media", MediaChain=MediaChain)
    _mk("app.chain.search", SearchChain=SearchChain)
    _mk("app.chain.torrents", TorrentsChain=TorrentsChain)

    return state


@dataclass
class _FakeSub:
    id: int
    name: str
    year: str = "2026"
    type: str = "电视剧"
    season: Optional[int] = 1
    media_source: str = "themoviedb"
    media_id: str = "10001"
    keyword: Optional[str] = None
    state: str = "R"
    best_version: int = 0
    best_version_full: int = 0
    filter_groups: List[str] = None  # type: ignore[assignment]
    sites: Optional[List[int]] = None
    custom_words: Optional[str] = None
    episode_group: Optional[str] = None
    total_episode: Optional[int] = 12
    start_episode: Optional[int] = 1
    lack_episode: Optional[int] = 9
    note: List[int] = None  # type: ignore[assignment]
    episode_priority: Dict[str, int] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.filter_groups is None:
            self.filter_groups = ["免费", "前置过滤", "日番"]
        if self.note is None:
            self.note = [1, 2, 3]
        if self.episode_priority is None:
            self.episode_priority = {"1": 88, "2": 88, "3": 98}


class SubscribeAuto4KTests(unittest.TestCase):
    def setUp(self) -> None:
        self.state = _install_mock_moviepilot_sdk()
        plugin_path = (
            Path(__file__).resolve().parents[3]
            / "plugins.v3"
            / "subscribeauto4k"
            / "__init__.py"
        )
        spec = importlib.util.spec_from_file_location("subscribeauto4k_test_mod", plugin_path)
        assert spec and spec.loader
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        self.mod = mod
        self.plugin = mod.SubscribeAuto4K()

    def test_tighten_from_transfer_history_and_requeue_low_res_episodes(self) -> None:
        sub = _FakeSub(id=101, name="测试番剧", note=[1, 2, 3], episode_priority={"1": 88, "2": 88, "3": 98})
        self.state["subscribes"] = [sub]
        self.state["transfer_records"] = [
            types.SimpleNamespace(status=True, episodes="E01", src="Show.S01E01.1080p.mkv", dest="S01E01.1080p.mkv"),
            types.SimpleNamespace(status=True, episodes="E02", src="Show.S01E02.1080p.mkv", dest="S01E02.1080p.mkv"),
            types.SimpleNamespace(status=True, episodes="E03", src="Show.S01E03.2160p.mkv", dest="S01E03.2160p.mkv"),
        ]
        self.plugin.init_plugin(
            {
                "enabled": True,
                "strict_group": "仅4K",
                "auto_upgrade": True,
                "upgrade_mode": "requeue",
                "upgrade_priority": 95,
            }
        )

        summary = self.plugin.sync_all_subscribes()
        self.assertEqual(summary["local_hits"], 1)
        self.assertEqual(summary["tightened"], 1)
        self.assertEqual(summary["upgraded_episodes"], 2)
        self.assertEqual(sub.filter_groups, ["免费", "前置过滤", "日番", "仅4K"])
        # E01 and E02 were 1080p -> removed from note; E03 was 2160p -> kept
        self.assertEqual(sub.note, [3])
        self.assertEqual(sub.lack_episode, 11)

        # Second scan must NOT re-upgrade E01/E02 endlessly
        summary2 = self.plugin.sync_all_subscribes()
        self.assertEqual(summary2["already_strict"], 1)
        self.assertEqual(summary2["upgraded_episodes"], 0)

        # Restore snapshot rolls back filter_groups and note cleanly
        res = self.plugin.api_restore(101)
        self.assertTrue(res["success"])
        self.assertEqual(sub.filter_groups, ["免费", "前置过滤", "日番"])
        self.assertEqual(sub.note, [1, 2, 3])

    def test_best_version_upgrade_mode_and_dry_run(self) -> None:
        sub = _FakeSub(id=202, name="热播剧", media_id="20202", note=[1, 2], episode_priority={"1": 87, "2": 99})
        self.state["subscribes"] = [sub]
        media_202 = sys.modules["app.sdk.media"].MediaInfo()
        media_202.title = "热播剧"
        media_202.tmdb_id = 20202
        media_202.douban_id = "36000001"
        self.state["cache_candidates"] = [
            types.SimpleNamespace(
                media_info=media_202,
                meta_info=sys.modules["app.sdk.media"].MetaInfo("热播剧", resource_pix="2160p", begin_season=1),
                torrent_info=types.SimpleNamespace(title="热播剧 S01E02 2160p WEB-DL", description="", site=1),
            )
        ]

        # Dry run must not mutate subscribe
        self.plugin.init_plugin({"enabled": True, "dry_run": True, "upgrade_mode": "best_version"})
        dry = self.plugin.sync_all_subscribes()
        self.assertEqual(dry["tightened"], 1)
        self.assertEqual(sub.filter_groups, ["免费", "前置过滤", "日番"])
        self.assertEqual(sub.best_version, 0)

        # Real run switches to best_version=1 and lowers priority only for E01
        real = self.plugin.sync_all_subscribes(dry_run_override=False)
        self.assertEqual(real["tightened"], 1)
        self.assertEqual(sub.filter_groups[-1], "仅4K")
        self.assertEqual(sub.best_version, 1)
        self.assertEqual(sub.episode_priority["1"], 1)
        self.assertEqual(sub.episode_priority["2"], 99)

    def test_realtime_transfer_event_tightens_immediately(self) -> None:
        sub = _FakeSub(id=303, name="新番4K", media_id="99999", note=[1], episode_priority={"1": 88})
        self.state["subscribes"] = [sub]
        self.plugin.init_plugin({"enabled": True, "enable_event_watch": True, "auto_upgrade": False})

        media = sys.modules["app.sdk.media"].MediaInfo()
        media.title = "新番4K"
        media.tmdb_id = 99999
        meta = sys.modules["app.sdk.media"].MetaInfo("新番4K", resource_pix="2160p", begin_season=1)
        event = sys.modules["app.sdk.events"].Event({"mediainfo": media, "meta": meta})
        self.plugin.on_transfer_complete(event)

        self.assertIn("仅4K", sub.filter_groups)

    def test_realtime_contract_object_events_tighten_immediately(self) -> None:
        sub1 = _FakeSub(id=304, name="合同事件整理4K", media_id="88881", note=[1], episode_priority={"1": 88})
        sub2 = _FakeSub(id=305, name="合同事件下载4K", media_id="88882", note=[1], episode_priority={"1": 88})
        self.state["subscribes"] = [sub1, sub2]
        self.plugin.init_plugin({"enabled": True, "enable_event_watch": True, "auto_upgrade": False})

        # Simulate V3 BaseEventData / snapshot object (non-dict) for TransferComplete
        transfer_payload = types.SimpleNamespace(
            mediainfo={"media_source": "themoviedb", "media_id": "88881", "title": "合同事件整理4K", "type": "电视剧"},
            meta={"begin_season": 1, "resource_pix": "2160p"},
            transferinfo=None,
            fileitem=None,
        )
        self.plugin.on_transfer_complete(sys.modules["app.sdk.events"].Event(transfer_payload))
        self.assertIn("仅4K", sub1.filter_groups)

        # Simulate V3 DownloadAddedContractData object (non-dict) for DownloadAdded
        dl_payload = types.SimpleNamespace(
            context=types.SimpleNamespace(
                media_info=types.SimpleNamespace(source=None, tmdb_id=88882, douban_id=None, title="合同事件下载4K", type="电视剧"),
                meta_info=types.SimpleNamespace(begin_season=1, resource_pix=None),
                torrent_info=types.SimpleNamespace(title="Show S01E02 2160p WEB-DL", description=""),
            )
        )
        self.plugin.on_download_added(sys.modules["app.sdk.events"].Event(dl_payload))
        self.assertIn("仅4K", sub2.filter_groups)

    def test_aborts_safely_when_strict_group_missing(self) -> None:
        sub = _FakeSub(id=404, name="安全测试")
        self.state["subscribes"] = [sub]
        self.plugin.init_plugin({"enabled": True, "strict_group": "不存在的组"})
        summary = self.plugin.sync_all_subscribes()
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(summary["tightened"], 0)
        self.assertEqual(self.state["updated_calls"], [])

    def test_empty_filter_groups_inherits_global_defaults_and_caps_upgrade_episodes(self) -> None:
        sub = _FakeSub(
            id=505,
            name="长篇年番",
            filter_groups=[],
            note=list(range(1, 31)),
            episode_priority={str(i): 88 for i in range(1, 31)},
            total_episode=100,
        )
        self.state["subscribes"] = [sub]
        self.state["transfer_records"] = [
            types.SimpleNamespace(status=True, episodes="E31", src="Show.S01E31.2160p.mkv", dest="S01E31.2160p.mkv"),
        ]
        self.plugin.init_plugin(
            {
                "enabled": True,
                "strict_group": "仅4K",
                "auto_upgrade": True,
                "upgrade_max_episodes": 5,
            }
        )
        summary = self.plugin.sync_all_subscribes(skip_search=True)
        self.assertEqual(summary["tightened"], 1)
        self.assertEqual(summary["upgraded_episodes"], 5)
        self.assertEqual(sub.filter_groups, ["免费", "前置过滤", "仅4K"])
        # Latest 5 episodes (26..30) were removed from note; earlier 1..25 untouched
        self.assertEqual(sub.note, list(range(1, 26)))

        # Subsequent scans MUST respect cumulative cap and not dig further back into 21..25
        summary2 = self.plugin.sync_all_subscribes(skip_search=True)
        self.assertEqual(summary2["already_strict"], 1)
        self.assertEqual(summary2["upgraded_episodes"], 0)
        self.assertEqual(sub.note, list(range(1, 26)))

        # Partial restore (keep_strict=True) restores note while keeping 仅4K
        res = self.plugin.api_restore(505, keep_strict=True)
        self.assertTrue(res["success"])
        self.assertEqual(sub.note, list(range(1, 31)))
        self.assertEqual(sub.filter_groups, ["免费", "前置过滤", "仅4K"])

    def test_dry_run_and_skip_search_do_not_block_active_search_cooldown(self) -> None:
        sub = _FakeSub(id=606, name="待探查剧", note=[], episode_priority={})
        self.state["subscribes"] = [sub]
        self.state["search_results"] = [
            types.SimpleNamespace(
                meta_info=sys.modules["app.sdk.media"].MetaInfo("待探查剧", resource_pix="2160p", begin_season=1),
                torrent_info=types.SimpleNamespace(title="待探查剧 S01E01 2160p WEB-DL", description="", site=1),
            )
        ]
        self.plugin.init_plugin(
            {
                "enabled": True,
                "enable_active_search": True,
                "check_ttl_hours": 6,
                "batch_limit": 5,
            }
        )
        # First a local-only dry run must NOT set search_ts cooldown
        dry = self.plugin.sync_all_subscribes(dry_run_override=True, skip_search=True)
        self.assertEqual(dry["search_calls"], 0)

        # Next real run immediately performs active search without waiting 6 hours
        real = self.plugin.sync_all_subscribes(dry_run_override=False, skip_search=False)
        self.assertEqual(real["search_calls"], 1)
        self.assertEqual(real["search_hits"], 1)
        self.assertIn("仅4K", sub.filter_groups)

    def test_form_page_and_service_contracts(self) -> None:
        self.plugin.init_plugin({"enabled": True, "scan_cron": "*/15 * * * *"})
        form, defaults = self.plugin.get_form()
        self.assertIsInstance(form, list)
        self.assertEqual(defaults["strict_group"], "仅4K")
        services = self.plugin.get_service()
        self.assertEqual(len(services), 1)
        self.assertEqual(services[0]["id"], "auto")
        page = self.plugin.get_page()
        self.assertIsInstance(page, list)


if __name__ == "__main__":
    unittest.main()
