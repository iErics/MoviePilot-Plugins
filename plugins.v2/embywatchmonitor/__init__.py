import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any

from app.core.config import settings
from app.core.event import eventmanager
from app.db.subscribe_oper import SubscribeOper
from app.helper.rule import RuleHelper
from app.log import logger
from app.modules.emby.emby import Emby
from app.scheduler import Scheduler
from app.schemas.types import EventType, MediaType


class EmbyWatchMonitor:
    # 插件基本信息
    plugin_name = "Emby追更"
    plugin_desc = "监控Emby用户正在观看的剧集，管理订阅规则并定期刷新更新"
    plugin_version = "1.0"
    plugin_author = "iErics"
    plugin_icon = "Bard_A.png"
    plugin_order = 20

    # 私有属性
    _enabled = False
    _cron = None
    _onlyonce = False
    _notify = False
    _check_interval = 6  # 默认6小时检查一次
    _refresh_interval = 30  # 默认30分钟刷新一次
    _rules_to_remove = []  # 需要移除的规则组名称
    _watched_shows = {}  # 缓存用户正在观看的剧集
    _last_check_time = None
    _last_refresh_time = None

    def init_plugin(self, config):
        """
        初始化插件
        """
        self._enabled = config.get("enabled", False)
        self._cron = config.get("cron")
        self._onlyonce = config.get("onlyonce", False)
        self._notify = config.get("notify", False)
        self._check_interval = config.get("check_interval", 6)
        self._refresh_interval = config.get("refresh_interval", 30)
        self._rules_to_remove = config.get("rules_to_remove", [])
        self._watched_shows = {}
        self._last_check_time = None
        self._last_refresh_time = None
        return True

    def get_state(self):
        """
        获取插件状态
        """
        return self._enabled

    def get_service(self):
        """
        定义插件服务（定时任务）
        """
        if not self._enabled:
            return []

        services = []

        # 检查服务 - 获取正在观看的剧集并管理订阅规则
        if self._onlyonce:
            # 一次性运行
            services.append({
                "id": "EmbyWatchMonitor_Check",
                "name": "Emby观看监控检查服务",
                "trigger": "interval",
                "func": self.check_watched_shows,
                "kwargs": {"seconds": 10}
            })
        elif self._cron:
            # 使用自定义cron表达式
            services.append({
                "id": "EmbyWatchMonitor_Check",
                "name": "Emby观看监控检查服务",
                "trigger": "cron",
                "func": self.check_watched_shows,
                "kwargs": {"expression": self._cron}
            })
        else:
            # 使用默认间隔
            services.append({
                "id": "EmbyWatchMonitor_Check",
                "name": "Emby观看监控检查服务",
                "trigger": "interval",
                "func": self.check_watched_shows,
                "kwargs": {"hours": self._check_interval}
            })

            # 刷新服务 - 定期刷新正在观看的剧集
        services.append({
            "id": "EmbyWatchMonitor_Refresh",
            "name": "Emby观看剧集刷新服务",
            "trigger": "interval",
            "func": self.refresh_watched_shows,
            "kwargs": {"minutes": self._refresh_interval}
        })

        return services

    def get_command(self):
        """
        定义插件命令
        """
        return [{
            "cmd": "refresh_watched",
            "desc": "刷新正在观看的剧集",
            "category": "订阅",
            "function": self.manual_refresh
        }]

    def get_form(self):
        """
        定义插件配置表单
        """
        # 获取所有可用的规则组
        rule_helper = RuleHelper()
        rule_groups = rule_helper.get_rule_groups()
        rule_options = [{"title": group.name, "value": group.name} for group in rule_groups]

        return [
            {
                "component": "switch",
                "label": "启用插件",
                "field": "enabled",
                "value": self._enabled
            },
            {
                "component": "input",
                "label": "Cron表达式",
                "field": "cron",
                "placeholder": "留空则使用默认间隔",
                "value": self._cron
            },
            {
                "component": "switch",
                "label": "仅运行一次",
                "field": "onlyonce",
                "value": self._onlyonce
            },
            {
                "component": "switch",
                "label": "发送通知",
                "field": "notify",
                "value": self._notify
            },
            {
                "component": "select",
                "label": "检查间隔(小时)",
                "field": "check_interval",
                "options": [
                    {"title": "1小时", "value": 1},
                    {"title": "3小时", "value": 3},
                    {"title": "6小时", "value": 6},
                    {"title": "12小时", "value": 12},
                    {"title": "24小时", "value": 24}
                ],
                "value": self._check_interval
            },
            {
                "component": "select",
                "label": "刷新间隔(分钟)",
                "field": "refresh_interval",
                "options": [
                    {"title": "10分钟", "value": 10},
                    {"title": "30分钟", "value": 30},
                    {"title": "60分钟", "value": 60},
                    {"title": "120分钟", "value": 120}
                ],
                "value": self._refresh_interval
            },
            {
                "component": "select",
                "label": "要移除的订阅规则",
                "field": "rules_to_remove",
                "options": rule_options,
                "multiple": True,
                "value": self._rules_to_remove
            }
        ]

    def get_dashboard(self) -> Tuple[Dict[str, int], Dict[str, Any], List[Dict[str, Any]]]:
        """
        定义插件仪表盘
        """
        # 列数配置
        cols = {
            "xs": 1,  # 手机屏幕显示1列
            "sm": 2,  # 小屏幕显示2列
            "md": 3,  # 中等屏幕显示3列
            "lg": 4,  # 大屏幕显示4列
            "xl": 5,  # 超大屏幕显示5列
        }

        # 全局属性
        attrs = {
            "title": "正在观看的剧集",
            "subtitle": f"共 {len(self._watched_shows)} 个剧集",
            "actions": [
                {
                    "type": "link",
                    "icon": "setting",
                    "text": "设置",
                    "link": f"/plugin/config/embywatchmonitor"
                }
            ]
        }

        # 页面元素
        elements = []

        # 添加正在观看的剧集卡片
        for key, show in self._watched_shows.items():
            title = show.get("title", "未知剧集")
            tmdbid = show.get("tmdbid")

            # 构建卡片元素
            card = {
                "type": "card",
                "title": title,
                "subtitle": f"TMDB ID: {tmdbid}" if tmdbid else "无TMDB ID",
                "actions": [
                    {
                        "type": "link",
                        "icon": "refresh",
                        "text": "刷新",
                        "link": f"/plugin/embywatchmonitor/refresh/{tmdbid}" if tmdbid else "#"
                    }
                ]
            }