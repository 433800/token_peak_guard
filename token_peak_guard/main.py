"""Token 计费高峰守卫（token_peak_guard）。

作用：
    按 DeepSeek 官方的「峰谷计费」时段，在高价的高峰时段内拦住除管理员以外的
    所有请求（对话 / 命令 / 定时任务），过了高峰自动放开，用来压低余额消耗。

官方口径（https://api-docs.deepseek.com/quick_start/pricing/）：
    高峰 = 北京时间 09:00-12:00、14:00-18:00，且仅在周一到周五；
    周末与中国法定节假日全天算空闲时段（半价）。
    所以判定要同时看「钟点 + 星期 + 节假日」，只看钟点会在周末误拦。

数据来源（自给自足，不依赖其它插件）：
    ``on_llm_response`` 里把 ``response.usage.total`` 累加到「当前小时」的桶里。

拦截点：
    1. ``event_message_type(ALL, priority=1000)``：优先级最高、最先执行。
       非管理员在高峰期直接 ``stop_event()``，后面的命令处理器、对话、其它插件
       一律不会执行（``StarRequestSubStage`` 见到 stopped 就 break）。
    2. ``on_llm_request``：兜底，拦掉定时任务、主动回复等不经过消息处理器的请求。

设计约束：
    - 任何异常都不能拖垮主流程，全部 try/except 兜底。
    - 统计文件原子写入（tmp + replace），最多 30 秒落一次盘。
    - 拦不拦只看官方峰谷时段（``peak_windows``，可改），不按用量自己推断。
    - 用量统计只服务于「每日日报」和「/高峰 统计」，不参与拦截判定。
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.provider import LLMResponse, ProviderRequest
from astrbot.api.star import Context, Star

DEFAULTS: dict[str, Any] = {
    # 优先设置（配置面板最上面那块的总控）
    "priority.enabled": True,
    "priority.hard_lock": False,
    "priority.hard_lock_text": "现在锁着呢，等管理员放开再来吧",
    # admin_only=只有管理员；global=跟全局访问控制白名单；whitelist=本插件名单；everyone=全放开只统计
    "priority.grant": "admin_only",
    "priority.whitelist": [],
    # 兼容旧版写在顶层的开关
    "enabled": True,
    # peak=按官方峰谷时段拦（默认）；off=只统计不拦截
    "mode": "peak",
    # 官方高峰：工作日 09:00-12:00、14:00-18:00（想自定义就改这里）
    "peak_windows": ["09:00-12:00", "14:00-18:00"],
    # 统计和日报回看的天数
    "analysis_days": 14,
    # 周末 / 法定节假日全天算空闲时段，不拦
    "skip_weekends": True,
    "holidays": [
        "2026-01-01~2026-01-03",
        "2026-02-15~2026-02-23",
        "2026-04-04~2026-04-06",
        "2026-05-01~2026-05-05",
        "2026-06-19~2026-06-21",
        "2026-09-25~2026-09-27",
        "2026-10-01~2026-10-07",
    ],
    "admin_ids": [],
    "debug_log": False,
    "bypass_platforms": ["webchat"],
    "notice_enabled": True,
    "notice_cooldown_minutes": 30,
    "notice_text": "现在是计费高峰（{windows}），{until} 再聊吧",
    "keep_days": 45,
    "daily_report_enabled": False,
    "daily_report_time": "21:00",
    "daily_report_target": "",
}

DAY_MIN = 1440


# ---------------------------------------------------------------- 纯函数工具


def _to_min(text: str) -> int | None:
    """把 ``22`` / ``22:30`` / ``24:00`` 转成分钟数（0~1440）。"""
    t = str(text).strip()
    if not t:
        return None
    if ":" in t:
        hh, _, mm = t.partition(":")
    else:
        hh, mm = t, "0"
    try:
        h = int(hh)
        m = int(mm)
    except ValueError:
        return None
    if not (0 <= h <= 24 and 0 <= m < 60):
        return None
    return min(h * 60 + m, DAY_MIN)


def norm_windows(raw: Any) -> list[tuple[int, int]]:
    """把 ``["23:00-02:00", ["08:00", "09:30"]]`` 归一化成 ``[(s, e)]``。

    支持跨零点（``s > e``，如 23:00-02:00）与 ``24:00`` 写法。
    """
    out: list[tuple[int, int]] = []
    if not raw:
        return out
    if isinstance(raw, str):
        raw = [raw]
    for item in raw:
        try:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                span = f"{item[0]}-{item[1]}"
            else:
                span = str(item)
        except Exception:
            continue
        span = (
            span.replace("：", ":")
            .replace("～", "-")
            .replace("~", "-")
            .replace("—", "-")
            .replace("－", "-")
            .replace("至", "-")
            .strip()
        )
        if "-" not in span:
            continue
        left, _, right = span.partition("-")
        s = _to_min(left)
        e = _to_min(right)
        if s is None or e is None or s == e:
            continue
        if (s, e) not in out:
            out.append((s, e))
    return out


def fmt_win(win: tuple[int, int]) -> str:
    s, e = win
    eh = 24 if e >= DAY_MIN else e // 60
    return f"{s // 60:02d}:{s % 60:02d}-{eh:02d}:{e % 60:02d}"


def in_window(cur: int, win: tuple[int, int]) -> bool:
    s, e = win
    if s < e:
        return s <= cur < e
    return cur >= s or cur < e


def minutes_until(cur: int, win: tuple[int, int]) -> int:
    return (win[1] - cur) % DAY_MIN or DAY_MIN


def parse_days(raw: Any) -> set[str]:
    """把节假日配置展开成日期字符串集合。

    支持 ``"2026-10-01"``、``"2026-10-01~2026-10-07"``、``"2026-10-01..10-07"``。
    区间分隔只用 ``~`` / ``..`` / ``至``，避免和日期里的 ``-`` 打架。
    """
    out: set[str] = set()
    if not raw:
        return out
    if isinstance(raw, str):
        raw = [raw]
    for item in raw:
        text = (
            str(item)
            .strip()
            .replace("～", "~")
            .replace("—", "~")
            .replace("－", "~")
            .replace("..", "~")
            .replace("至", "~")
        )
        if not text:
            continue
        try:
            if "~" in text:
                left, _, right = text.partition("~")
                start = date.fromisoformat(left.strip())
                end = date.fromisoformat(right.strip())
                if end < start:
                    start, end = end, start
                for i in range((end - start).days + 1):
                    out.add((start + timedelta(days=i)).isoformat())
            else:
                out.add(date.fromisoformat(text).isoformat())
        except ValueError:
            continue
    return out


def compress_days(days: Any) -> list[str]:
    """把日期集合压成尽量短的区间写法，方便塞回配置。"""
    objs: list[date] = []
    for s in sorted({str(d) for d in (days or [])}):
        try:
            objs.append(date.fromisoformat(s))
        except ValueError:
            continue
    out: list[str] = []
    i = 0
    while i < len(objs):
        j = i
        while j + 1 < len(objs) and (objs[j + 1] - objs[j]).days == 1:
            j += 1
        out.append(
            objs[i].isoformat()
            if j == i
            else f"{objs[i].isoformat()}~{objs[j].isoformat()}"
        )
        i = j + 1
    return out


WEEK_CN = ("周一", "周二", "周三", "周四", "周五", "周六", "周日")


def human(n: float) -> str:
    """数字缩写：1234 → 1.2k；123456 → 12.3万。"""
    try:
        n = float(n)
    except Exception:
        return "0"
    if n >= 100_000_000:
        return f"{n / 100_000_000:.2f}亿"
    if n >= 10_000:
        return f"{n / 10_000:.1f}万"
    if n >= 1000:
        return f"{n / 1000:.1f}k"
    return str(int(n))


class Main(Star):
    """插件入口。"""

    def __init__(self, context: Context, config: dict | None = None) -> None:
        super().__init__(context)
        self.context = context
        self.config: dict = config if config is not None else {}
        self._stats: dict[str, Any] = {"version": 1, "buckets": {}}
        self._loaded = False
        self._dirty = False
        self._last_save = 0.0
        self._notice_ts: dict[str, float] = {}
        self._cache: tuple[float, list[tuple[int, int]], str] | None = None
        self._holiday_cache: tuple[str, set[str]] | None = None
        self._report_task: asyncio.Task | None = None
        self._last_report_day = ""

    # ------------------------------------------------------------ 生命周期

    def _migrate_config(self) -> None:
        """把老配置（mode=manual/fixed/auto + manual_windows/fixed_windows）并成 peak 模式。"""
        try:
            if self._raw_get("peak_windows") is None:
                old = self._raw_get("manual_windows") or self._raw_get("fixed_windows")
                wins = norm_windows(old) if old else []
                self._set_cfg(
                    "peak_windows",
                    [fmt_win(w) for w in wins] or list(DEFAULTS["peak_windows"]),
                )
            mode = str(self._raw_get("mode") or "").strip().lower()
            if mode not in ("peak", "off"):
                self._set_cfg("mode", "peak")
            if isinstance(self.config, dict):
                dropped = False
                for key in ("manual_windows", "fixed_windows", "peak_factor", "top_k_hours", "min_tokens"):
                    if key in self.config:
                        self.config.pop(key, None)
                        dropped = True
                if dropped:
                    save = getattr(self.config, "save_config", None)
                    if callable(save):
                        save()
            self._cache = None
        except Exception as e:
            logger.warning(f"[peak_guard] 迁移旧配置失败（不影响使用）: {e}")

    async def initialize(self) -> None:
        self._migrate_config()
        self._load_stats()
        self._dlog("=== 插件已加载 v-diag ===")
        try:
            self._report_task = asyncio.create_task(self._report_loop())
        except Exception as e:
            logger.warning(f"[peak_guard] 日报任务启动失败: {e}")
        try:
            self._register_web_api()
        except Exception as e:
            logger.warning(f"[peak_guard] 面板接口注册失败: {e}")
        wins, src = self._resolve_windows(force=True)
        try:
            st = self._state()
            state = (
                "🔒 锁死中"
                if st.get("locked")
                else ("高峰期" if st["blocked"] else "非高峰")
            )
            logger.info(
                f"[peak_guard] 已加载 · 模式 {self._cfg('mode')} · "
                f"高峰 {('、'.join(fmt_win(w) for w in wins) or '无')}（{src}）· "
                f"今天{st['day_note']} · 现在{state}"
            )
        except Exception as e:
            logger.info(f"[peak_guard] 已加载（状态输出失败: {e}）")

    async def terminate(self) -> None:
        task = self._report_task
        if task is not None and not task.done():
            task.cancel()
        self._flush(force=True)

    # ------------------------------------------------------------ 配置读取

    def _raw_get(self, key: str) -> Any:
        """按 ``a.b.c`` 取嵌套配置，取不到返回 None。"""
        try:
            node: Any = self.config
            for part in key.split("."):
                if not isinstance(node, dict) or part not in node:
                    return None
                node = node[part]
            return node
        except Exception:
            return None

    def _cfg(self, key: str, default: Any = None) -> Any:
        val = self._raw_get(key)
        if val is not None:
            return val
        if "." in key:
            # 旧版把开关写在顶层（enabled），读不到新位置时退化过去
            val = self._raw_get(key.split(".", 1)[1])
            if val is not None:
                return val
        if key in DEFAULTS:
            return DEFAULTS[key]
        return default

    def _set_cfg(self, key: str, value: Any) -> None:
        try:
            if isinstance(self.config, dict):
                parts = key.split(".")
                node: Any = self.config
                for part in parts[:-1]:
                    nxt = node.get(part)
                    if not isinstance(nxt, dict):
                        nxt = {}
                        node[part] = nxt
                    node = nxt
                node[parts[-1]] = value
                save = getattr(self.config, "save_config", None)
                if callable(save):
                    save()
        except Exception as e:
            logger.warning(f"[peak_guard] 保存配置失败 {key}: {e}")
        self._cache = None

    def _enabled(self) -> bool:
        return bool(self._cfg("priority.enabled", True))

    def _hard_lock(self) -> bool:
        return bool(self._cfg("priority.hard_lock", False))

    def _grant_mode(self) -> str:
        """放行范围：admin_only / global / whitelist / everyone。"""
        val = str(self._cfg("priority.grant", "admin_only") or "admin_only").lower()
        return val if val in ("admin_only", "global", "whitelist", "everyone") else "admin_only"

    def _whitelist_ids(self) -> set[str]:
        raw = self._cfg("priority.whitelist", []) or []
        try:
            return {str(x).strip() for x in raw if str(x).strip()}
        except Exception:
            return set()

    # ------------------------------------------------- 全局访问控制白名单

    def _data_dir(self) -> str:
        """AstrBot 的 data 目录（插件目录往上两层）。"""
        here = os.path.dirname(os.path.abspath(__file__))
        return os.path.abspath(os.path.join(here, "..", ".."))

    def _global_platform_settings(self) -> dict:
        """读 AstrBot 全局 platform_settings，按 mtime 缓存，改了就重读。"""
        path = os.path.join(self._data_dir(), "cmd_config.json")
        try:
            mtime = os.stat(path).st_mtime
        except Exception:
            return {}
        cache = getattr(self, "_gps_cache", None)
        if cache and cache[0] == mtime:
            return cache[1]
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                ps = (json.load(f) or {}).get("platform_settings") or {}
        except Exception:
            return {}
        if not isinstance(ps, dict):
            return {}
        self._gps_cache = (mtime, ps)
        return ps

    def _global_wl_enabled(self) -> bool:
        return bool(self._global_platform_settings().get("enable_id_white_list", False))

    def _global_whitelist_ids(self) -> set[str]:
        """全局白名单里的 ID（"平台:类型:ID" 只留最后一段）。未启用则返回空集。"""
        ps = self._global_platform_settings()
        if not ps.get("enable_id_white_list"):
            return set()
        out: set[str] = set()
        raw = ps.get("id_whitelist") or []
        if isinstance(raw, (str, int)):
            raw = [raw]
        try:
            for item in raw:
                s = str(item).strip()
                if s:
                    out.add(s.rsplit(":", 1)[-1].strip())
        except Exception:
            pass
        return out

    # ------------------------------------------------------------ 时间

    def _now(self) -> datetime:
        tz = "Asia/Shanghai"
        try:
            cfg = self.context.get_config()
            tz = str(cfg.get("timezone") or tz)
        except Exception:
            pass
        try:
            return datetime.now(ZoneInfo(tz))
        except Exception:
            return datetime.now()

    # ------------------------------------------------------------ 高峰日判定

    def _holiday_set(self) -> set[str]:
        """节假日日期集合，配置变了才重新展开。"""
        raw = self._cfg("holidays", []) or []
        try:
            sig = json.dumps(raw, ensure_ascii=False, sort_keys=True)
        except Exception:
            sig = repr(raw)
        if self._holiday_cache and self._holiday_cache[0] == sig:
            return self._holiday_cache[1]
        days = parse_days(raw)
        self._holiday_cache = (sig, days)
        return days

    def _day_status(self, day: date) -> tuple[bool, str]:
        """返回 (这天算不算计费高峰日, 说明)。

        官方：高峰只在周一至周五，法定节假日和周末全天都是便宜的空闲时段。
        """
        if day.isoformat() in self._holiday_set():
            return False, "法定节假日"
        if bool(self._cfg("skip_weekends", True)) and day.weekday() >= 5:
            return False, "周末"
        return True, "工作日"

    # ------------------------------------------------------------ 统计存储

    def _stats_path(self) -> str:
        try:
            base = str(self.get_data_dir())
        except Exception:
            base = os.getcwd()
        os.makedirs(base, exist_ok=True)
        return os.path.join(base, "peak_stats.json")

    def _load_stats(self) -> None:
        path = self._stats_path()
        try:
            if os.path.exists(path):
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, dict) and isinstance(data.get("buckets"), dict):
                    self._stats = data
        except Exception as e:
            logger.warning(f"[peak_guard] 读取统计失败，重新开始: {e}")
            self._stats = {"version": 1, "buckets": {}}
        self._loaded = True
        self._prune()

    def _flush(self, force: bool = False) -> None:
        if not self._loaded:
            return
        if not (force or self._dirty):
            return
        path = self._stats_path()
        tmp = path + ".tmp"
        try:
            self._stats["updated_at"] = int(time.time())
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._stats, f, ensure_ascii=False)
            os.replace(tmp, path)
            self._dirty = False
            self._last_save = time.monotonic()
        except Exception as e:
            logger.warning(f"[peak_guard] 写入统计失败: {e}")

    def _prune(self) -> None:
        buckets: dict = self._stats.get("buckets") or {}
        keep = int(self._cfg("keep_days", 45) or 45)
        cutoff = (self._now().date() - timedelta(days=keep)).isoformat()
        stale = [k for k in buckets if str(k)[:10] < cutoff]
        for k in stale:
            buckets.pop(k, None)

    def _add_tokens(self, total: int) -> None:
        if total <= 0:
            return
        key = self._now().strftime("%Y-%m-%d %H")
        buckets = self._stats.setdefault("buckets", {})
        buckets[key] = int(buckets.get(key, 0)) + int(total)
        self._dirty = True
        self._prune()
        if time.monotonic() - self._last_save > 30:
            self._flush()

    # ------------------------------------------------------------ 高峰判定

    def _mode(self) -> str:
        """peak=按官方峰谷时段拦；off=只统计不拦（老配置的 manual/fixed/auto 一律当 peak）。"""
        val = str(self._cfg("mode", "peak") or "peak").strip().lower()
        return "off" if val == "off" else "peak"

    def _resolve_windows(self, force: bool = False) -> tuple[list[tuple[int, int]], str]:
        """返回 (时段列表, 来源说明)。结果缓存 120 秒。"""
        if not force and self._cache and time.monotonic() - self._cache[0] < 120:
            return self._cache[1], self._cache[2]

        official = norm_windows(DEFAULTS["peak_windows"])
        wins = norm_windows(self._cfg("peak_windows", DEFAULTS["peak_windows"])) or official
        src = "官方峰谷" if wins == official else "自定义"

        self._cache = (time.monotonic(), wins, src)
        return wins, src

    def _state(self) -> dict[str, Any]:
        wins, src = self._resolve_windows()
        now = self._now()
        cur = now.hour * 60 + now.minute
        mode = self._mode()
        peak_day, day_note = self._day_status(now.date())
        on = self._enabled()
        # 锁死模式：无视时段、无视周末节假日，全天拦（总开关关掉则不生效）
        locked = on and self._hard_lock()
        armed = on and peak_day and mode != "off"
        hit = [w for w in wins if in_window(cur, w)] if armed else []
        blocked = locked or bool(hit)
        until = ""
        if locked:
            until = ""
        elif hit:
            mins = min(minutes_until(cur, w) for w in hit)
            until = (now + timedelta(minutes=mins)).strftime("%H:%M")
        return {
            "blocked": blocked,
            "locked": locked,
            "windows": [fmt_win(w) for w in wins],
            "until": until,
            "source": src,
            "now": now,
            "peak_day": peak_day,
            "day_note": day_note,
        }

    # ------------------------------------------------------------ 权限

    def _is_admin(self, event: AstrMessageEvent) -> bool:
        try:
            if event.is_admin():
                return True
        except Exception:
            pass
        sid = ""
        try:
            sid = str(event.get_sender_id())
        except Exception:
            pass
        admin_ids = self._cfg("admin_ids", []) or []
        try:
            return bool(sid) and sid in {str(x) for x in admin_ids}
        except Exception:
            return False

    def _platform_bypass(self, event: AstrMessageEvent) -> bool:
        try:
            name = str(event.get_platform_name() or "")
        except Exception:
            return False
        bypass = self._cfg("bypass_platforms", []) or []
        try:
            return name in {str(x) for x in bypass}
        except Exception:
            return False

    def _allowed(self, event: AstrMessageEvent) -> bool:
        """这个人在拦截规则下算不算放行。"""
        if self._is_admin(event) or self._platform_bypass(event):
            return True
        mode = self._grant_mode()
        if mode == "everyone":
            return True
        if mode == "whitelist":
            try:
                sid = str(event.get_sender_id() or "").strip()
            except Exception:
                sid = ""
            return bool(sid) and sid in self._whitelist_ids()
        if mode == "global":
            gids = self._global_whitelist_ids()
            if not gids:
                return False
            ids = []
            try:
                ids.append(str(event.get_sender_id() or "").strip())
            except Exception:
                pass
            try:
                ids.append(str(event.get_group_id() or "").strip())
            except Exception:
                pass
            return any(i and i in gids for i in ids)
        return False

    # ------------------------------------------------------------ 拦截钩子

    def _dlog(self, msg: str) -> None:
        """诊断日志：直接落盘，不依赖 logger 配置。超过 2MB 自动重来。

        开关是 debug_log，默认关：日志会记发送者 ID 和消息片段，别长期开着。
        """
        if not self._cfg("debug_log", False):
            return
        try:
            path = os.path.join(self._data_dir(), "peak_guard_diag.log")
            if os.path.exists(path) and os.path.getsize(path) > 2 * 1024 * 1024:
                os.replace(path, path + ".old")
            with open(path, "a", encoding="utf-8") as f:
                f.write(f"{self._now().strftime('%Y-%m-%d %H:%M:%S')} {msg}\n")
        except Exception:
            pass

    @filter.event_message_type(filter.EventMessageType.ALL, priority=1000)
    async def guard_message(self, event: AstrMessageEvent) -> None:
        """高峰期内，非管理员的消息一律拦下。"""
        try:
            try:
                who = (
                    f"平台={event.get_platform_name()} 发送者={event.get_sender_id()} "
                    f"会话={event.unified_msg_origin} 私聊={event.is_private_chat()} "
                    f"文本={event.message_str[:30]!r}"
                )
            except Exception:
                who = "（事件信息读取失败）"
            self._dlog(f"进入守卫 | {who}")
            if self._is_admin(event):
                self._remember_umo(event)
            if not self._enabled():
                self._dlog("  放行：守卫已关闭")
                return
            st = self._state()
            self._dlog(
                f"  判定 locked={st['locked']} blocked={st['blocked']} "
                f"peak_day={st['peak_day']} grant={self._grant_mode()} "
                f"admin={self._is_admin(event)} bypass={self._platform_bypass(event)}"
            )
            if st["locked"]:
                # 锁死模式：无视放行范围，只认管理员和平台绕过
                if self._is_admin(event) or self._platform_bypass(event):
                    self._dlog("  放行：锁死但属于管理员/平台绕过")
                    return
            elif self._allowed(event) or not st["blocked"]:
                self._dlog("  放行：不在拦截条件内")
                return
            event.stop_event()
            self._dlog("  已拦截")
            key = self._notice_ready(event)
            if key is None:
                self._dlog("  提示跳过：冷却中或提示已关闭")
                return
            try:
                await event.send(MessageChain().message(self._notice_text(st)))
                self._notice_mark(key)
                self._dlog(f"  提示已发送 → {key} | {self._notice_text(st)!r}")
            except Exception as e:
                self._dlog(f"  提示发送失败: {e}")
        except Exception as e:
            self._dlog(f"  守卫异常: {e}")

    @filter.on_llm_request()
    async def guard_llm_request(
        self, event: AstrMessageEvent, req: ProviderRequest
    ) -> None:
        """兜底：拦掉不经过消息处理器的 LLM 请求（定时任务、主动回复等）。"""
        try:
            if not self._enabled():
                return
            st = self._state()
            if st["locked"]:
                if self._is_admin(event) or self._platform_bypass(event):
                    return
            elif self._allowed(event) or not st["blocked"]:
                return
            event.stop_event()
        except Exception as e:
            logger.warning(f"[peak_guard] LLM 拦截异常（已忽略）: {e}")

    @filter.on_llm_response()
    async def record_usage(
        self, event: AstrMessageEvent, response: LLMResponse
    ) -> None:
        """记录本次请求的 token 消耗到当前小时的桶里。"""
        try:
            usage = getattr(response, "usage", None)
            if usage is None:
                return
            total = int(getattr(usage, "total", 0) or 0)
            if total <= 0:
                total = (
                    int(getattr(usage, "input_other", 0) or 0)
                    + int(getattr(usage, "input_cached", 0) or 0)
                    + int(getattr(usage, "output", 0) or 0)
                )
            if total > 0:
                self._add_tokens(total)
        except Exception as e:
            logger.debug(f"[peak_guard] 记录用量失败: {e}")

    # ------------------------------------------------------------ 每日日报

    def _remember_umo(self, event: AstrMessageEvent) -> None:
        """记住管理员最近说话的会话，日报默认发到这里。"""
        try:
            umo = str(event.unified_msg_origin or "")
        except Exception:
            return
        if umo and self._stats.get("last_admin_umo") != umo:
            self._stats["last_admin_umo"] = umo
            self._dirty = True

    async def _report_loop(self) -> None:
        """每分钟看一眼，到点就发前一天的用量日报。"""
        while True:
            try:
                await asyncio.sleep(20)
                if not self._cfg("daily_report_enabled", False):
                    continue
                now = self._now()
                raw = str(self._cfg("daily_report_time", "21:00") or "21:00")
                try:
                    th, tm = (int(x) for x in raw.replace("：", ":").split(":"))
                except Exception:
                    continue
                if now.hour != th or now.minute != tm:
                    continue
                today = now.date().isoformat()
                if str(self._stats.get("last_report_day") or "") == today:
                    continue
                self._stats["last_report_day"] = today
                self._dirty = True
                self._flush(force=True)
                await self._send_report(now)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning(f"[peak_guard] 日报循环异常（已忽略）: {e}")
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    raise

    async def _send_report(self, now: datetime) -> None:
        umo = str(self._cfg("daily_report_target", "") or "").strip()
        if not umo:
            umo = str(self._stats.get("last_admin_umo") or "")
        if not umo:
            logger.info("[peak_guard] 日报跳过：还没有可用的会话，先在面板里填 target")
            return
        try:
            text = self._daily_text(now.date() - timedelta(days=1))
            await self.context.send_message(umo, MessageChain().message(text))
            logger.info(f"[peak_guard] 日报已发送 → {umo}")
        except Exception as e:
            logger.warning(f"[peak_guard] 日报发送失败: {e}")

    def _daily_text(self, day) -> str:
        buckets: dict = self._stats.get("buckets") or {}
        prefix = day.isoformat()
        vals = [0] * 24
        for key, val in buckets.items():
            text = str(key)
            if not text.startswith(prefix):
                continue
            _, _, hour = text.partition(" ")
            try:
                hi = int(hour)
                vi = int(val)
            except (ValueError, TypeError):
                continue
            if 0 <= hi < 24 and vi > 0:
                vals[hi] += vi
        total = sum(vals)
        peak_day, day_note = self._day_status(day)
        head = (
            f"【{prefix} 用量日报】共 {human(total)} token"
            f"（{WEEK_CN[day.weekday()]}·{day_note}）"
        )
        if total <= 0:
            return head + "\n这天没记录到用量。"

        wins, src = self._resolve_windows(force=True)
        peak_hours = (
            [h for h in range(24) if any(in_window(h * 60, w) for w in wins)]
            if peak_day
            else []
        )
        gap = max(vals) or 1
        lines = [head]
        for h in range(24):
            if vals[h] <= 0:
                continue
            bar = "█" * max(1, round(vals[h] / gap * 8))
            lines.append(f"{h:02d}点 {human(vals[h]):>7} {bar}{'  峰' if h in peak_hours else ''}")

        peak = sum(vals[h] for h in peak_hours)
        rest = total - peak
        top_h = max(range(24), key=lambda h: vals[h])
        if peak_day and vals[top_h] > 0 and top_h not in peak_hours:
            lines.append(
                f"最忙的是 {top_h:02d}点（{human(vals[top_h])}），现在不在高峰时段里。"
            )
        ph = float(len(peak_hours))
        rh = 24.0 - ph
        if ph and rh:
            lines.append(
                f"高峰 {human(peak)}（{human(peak / ph)}/小时）  "
                f"谷时 {human(rest)}（{human(rest / rh)}/小时）"
            )
            if peak / ph <= rest / rh:
                lines.append("高峰每小时反而不比谷时高，时段可能划得不准，看一眼上面的分布。")
        elif not peak_day:
            lines.append("这天不在高峰（" + day_note + "），全天都是空闲时段。")
        else:
            lines.append("还没设高峰时段。")
        lines.append(f"（{src}）")
        return "\n".join(lines)

    # ------------------------------------------------------------ 提示

    def _notice_text(self, st: dict[str, Any]) -> str:
        if st.get("locked"):
            default = str(DEFAULTS["priority.hard_lock_text"])
            tpl = str(self._cfg("priority.hard_lock_text", default))
        else:
            default = str(DEFAULTS["notice_text"])
            tpl = str(self._cfg("notice_text", default))
        text = (
            tpl.replace("{windows}", "、".join(st["windows"]) or "-")
            .replace("{until}", st["until"] or "-")
            .replace("{now}", st["now"].strftime("%H:%M"))
        )
        return text.strip() or default

    def _notice_ready(self, event: AstrMessageEvent) -> str | None:
        """能发提示就返回会话 key，冷却中/提示关着返回 None。"""
        if not self._cfg("notice_enabled", True):
            return None
        cooldown = max(0, int(self._cfg("notice_cooldown_minutes", 30) or 0)) * 60
        try:
            key = str(event.unified_msg_origin)
        except Exception:
            key = "default"
        if cooldown and time.monotonic() - self._notice_ts.get(key, 0.0) < cooldown:
            return None
        return key

    def _notice_mark(self, key: str) -> None:
        """只有真发出去了才记时间，失败不占冷却。"""
        self._notice_ts[key] = time.monotonic()

    # ------------------------------------------------------------ 命令

    @filter.command("高峰", alias={"peak"})
    async def peak_cmd(self, event: AstrMessageEvent):
        """高峰守卫管理命令（仅管理员）。"""
        if not self._is_admin(event):
            yield event.plain_result("这个只有管理员能用哦。")
            return

        raw = ""
        try:
            raw = (event.get_message_str() or "").strip()
        except Exception:
            raw = ""
        body = raw.lstrip("/／").strip()
        for name in ("高峰", "peak"):
            if body.startswith(name):
                body = body[len(name) :].strip()
                break
        parts = body.split()
        sub = parts[0].lower() if parts else ""
        args = parts[1:]

        if sub in ("", "状态", "status"):
            yield event.plain_result(self._status_text())
            return
        if sub in ("统计", "stats", "分析"):
            yield event.plain_result(self._stats_text())
            return
        if sub in ("帮助", "help", "?", "？"):
            yield event.plain_result(self._help_text())
            return
        if sub in ("开关", "on", "off", "切换"):
            yield event.plain_result(self._cmd_switch(sub, args))
            return
        if sub in ("锁死", "lock", "封锁"):
            yield event.plain_result(self._cmd_lock(args))
            return
        if sub in ("放行", "grant", "范围"):
            yield event.plain_result(self._cmd_grant(args))
            return
        if sub in ("白名单", "名单", "whitelist", "wl"):
            yield event.plain_result(self._cmd_whitelist(args))
            return
        if sub in ("模式", "mode"):
            yield event.plain_result(self._cmd_mode(args))
            return
        if sub in ("窗口", "时段", "windows"):
            yield event.plain_result(self._cmd_windows(args))
            return
        if sub in ("假日", "节假日", "holiday", "holidays"):
            yield event.plain_result(self._cmd_holiday(args))
            return
        if sub in ("周末", "weekend", "weekends"):
            yield event.plain_result(self._cmd_weekend(args))
            return
        if sub in ("日报", "daily", "报告"):
            if args and args[0] in ("现在", "立即", "test", "试试"):
                await self._send_report(self._now())
                yield event.plain_result("发了一份昨天的给你，看看格式。")
                return
            yield event.plain_result(self._cmd_daily(args))
            return
        if sub in ("重算", "刷新", "reload"):
            self._cache = None
            yield event.plain_result("重算好了。\n" + self._status_text())
            return
        if sub in ("清除", "清空", "reset"):
            if args and args[0] in ("确认", "yes", "ok"):
                self._stats = {"version": 1, "buckets": {}}
                self._dirty = True
                self._flush(force=True)
                self._cache = None
                yield event.plain_result("统计清空了。")
            else:
                yield event.plain_result("会清空全部历史统计。确认请发：/高峰 清除 确认")
            return

        yield event.plain_result("没听懂这个子命令。\n" + self._help_text())

    # ------------------------------------------------------------ 命令实现

    def _cmd_switch(self, sub: str, args: list[str]) -> str:
        if sub == "切换":
            new = not self._enabled()
        elif sub == "on":
            new = True
        elif sub == "off":
            new = False
        else:
            word = args[0].lower() if args else ""
            if word in ("off", "关", "关闭", "0", "false"):
                new = False
            elif word in ("on", "开", "开启", "1", "true"):
                new = True
            else:
                return "用法：/高峰 开关 on|off"
        self._set_cfg("priority.enabled", new)
        if not new:
            return "高峰守卫已关闭（只统计不拦截）。"
        if self._grant_mode() == "everyone":
            return "高峰守卫已开启。\n不过放行范围是「全部人」，现在等于只统计不拦人。"
        return "高峰守卫已开启。\n" + self._grant_line()

    def _cmd_lock(self, args: list[str]) -> str:
        """锁死模式：谷时也拦，只放管理员。"""
        word = (args[0].lower() if args else "").strip()
        if word in ("文案", "文本", "text", "回复"):
            text = " ".join(args[1:]).strip()
            if not text:
                cur = str(self._cfg("priority.hard_lock_text", DEFAULTS["priority.hard_lock_text"]))
                return "锁死时的回复是：\n" + cur + "\n\n用法：/高峰 锁死 文案 现在锁着呢"
            self._set_cfg("priority.hard_lock_text", text)
            return "锁死回复已改成：\n" + text
        cur = self._hard_lock()
        if not word:
            return (
                f"锁死模式：{'开（谷时也拦，只有管理员能说话）' if cur else '关（按高峰时段判定）'}"
                + "\n用法：/高峰 锁死 on|off|切换｜/高峰 锁死 文案 <内容>"
            )
        if word in ("on", "开", "开启", "1", "true"):
            new = True
        elif word in ("off", "关", "关闭", "0", "false"):
            new = False
        elif word in ("切换", "toggle"):
            new = not cur
        else:
            return "用法：/高峰 锁死 on|off|切换｜/高峰 锁死 文案 <内容>"
        self._set_cfg("priority.hard_lock", new)
        if new:
            return (
                "锁死模式已开。\n从现在起不管是不是高峰，除管理员外的聊天、命令、定时任务全拦住。"
            )
        return "锁死模式已关，回到按高峰时段判定。"

    GRANT_CN = {
        "admin_only": "只有管理员能用（其他人都拦）",
        "global": "跟全局访问控制白名单一致",
        "whitelist": "管理员 + 本插件名单里的人能用",
        "everyone": "全部人放开，只统计不拦",
    }

    def _grant_line(self) -> str:
        mode = self._grant_mode()
        line = "放行：" + self.GRANT_CN.get(mode, mode)
        if mode == "whitelist":
            n = len(self._whitelist_ids())
            line += f"（名单 {n} 人）"
        elif mode == "global":
            if self._global_wl_enabled():
                line += f"（全局名单 {len(self._global_whitelist_ids())} 个会话）"
            else:
                line += "（全局白名单没开，这档等于只有管理员能用）"
        return line

    def _cmd_grant(self, args: list[str]) -> str:
        word = (args[0].lower() if args else "").strip()
        mapping = {
            "admin": "admin_only",
            "admin_only": "admin_only",
            "管理员": "admin_only",
            "只有管理员": "admin_only",
            "global": "global",
            "全局": "global",
            "全局白名单": "global",
            "跟全局": "global",
            "whitelist": "whitelist",
            "白名单": "whitelist",
            "everyone": "everyone",
            "全部": "everyone",
            "所有人": "everyone",
            "全部人": "everyone",
        }
        if not word:
            return "现在" + self._grant_line() + "\n用法：/高峰 放行 管理员|全局|白名单|全部"
        if word not in mapping:
            return "用法：/高峰 放行 管理员|全局|白名单|全部\n现在" + self._grant_line()
        new = mapping[word]
        self._set_cfg("priority.grant", new)
        msg = "放行范围已改成：" + self.GRANT_CN[new] + "。"
        if new == "whitelist" and not self._whitelist_ids():
            msg += "\n不过名单还是空的，先用 /高峰 白名单 加 <ID> 加几个人，不然等于只有管理员能用。"
        if new == "global" and not self._global_wl_enabled():
            msg += "\n不过全局访问控制里的白名单现在是关着的，这档等于只有管理员能用。"
        return msg

    def _cmd_whitelist(self, args: list[str]) -> str:
        ids = self._whitelist_ids()
        if not args:
            mode = self._grant_mode()
            head = f"放行范围：{self.GRANT_CN.get(mode, mode)}"
            if not ids:
                return head + "\n白名单是空的。\n用法：/高峰 白名单 加 123456"
            body = "\n".join(sorted(ids))
            return head + f"\n白名单（{len(ids)} 人）：\n" + body + "\n用法：/高峰 白名单 加|删 <ID>"
        op = args[0].lower()
        rest = [a.strip() for a in args[1:] if a.strip()]
        if op in ("加", "添加", "add") and rest:
            new_ids = ids | set(rest)
            self._set_cfg("priority.whitelist", sorted(new_ids))
            extra = ""
            if self._grant_mode() != "whitelist":
                extra = "\n现在放行范围不是「白名单」，名单暂时不生效：/高峰 放行 白名单"
            if self._global_wl_enabled():
                gids = self._global_whitelist_ids()
                missing = [i for i in rest if i not in gids]
                if missing:
                    extra += "\n注意：" + "、".join(missing) + " 不在全局访问控制白名单里，消息根本到不了插件，加了也没用。"
            return f"加好了，现在名单 {len(new_ids)} 人：" + "、".join(rest) + extra
        if op in ("删", "删除", "移除", "del", "rm", "remove") and rest:
            new_ids = ids - set(rest)
            self._set_cfg("priority.whitelist", sorted(new_ids))
            return f"删掉了，现在名单 {len(new_ids)} 人。"
        if op in ("清空", "clear", "reset"):
            self._set_cfg("priority.whitelist", [])
            return "白名单清空了。"
        return "用法：/高峰 白名单 加|删 <ID>｜/高峰 白名单 清空"

    def _cmd_mode(self, args: list[str]) -> str:
        """模式：peak=官方峰谷拦（默认）；off=只统计不拦。"""
        word = (args[0].lower() if args else "").strip()
        mapping = {
            "peak": "peak",
            "峰谷": "peak",
            "官方": "peak",
            "手动": "peak",
            "固定": "peak",
            "自动": "peak",
            "off": "off",
            "关": "off",
            "只统计": "off",
        }
        if word not in mapping:
            cur = "peak（按官方峰谷拦）" if self._mode() == "peak" else "off（只统计不拦）"
            return f"用法：/高峰 模式 peak|off\n现在是：{cur}"
        mode = mapping[word]
        if self._raw_get("peak_windows") is None:
            self._set_cfg("peak_windows", list(DEFAULTS["peak_windows"]))
        self._set_cfg("mode", mode)
        self._cache = None
        if mode == "off":
            return "模式切成 off：只统计，不拦任何人。"
        wins, src = self._resolve_windows(force=True)
        return "模式切成 peak：" + "、".join(fmt_win(w) for w in wins) + f"（{src}）"

    def _cmd_windows(self, args: list[str]) -> str:
        """看/改峰谷时段。"""
        usage = "用法：/高峰 时段 09:00-12:00 14:00-18:00 ｜ /高峰 时段 默认"
        if not args:
            wins, src = self._resolve_windows(force=True)
            return (
                "当前高峰时段："
                + ("、".join(fmt_win(w) for w in wins) or "未设置")
                + f"（{src}）\n"
                + usage
            )
        if args[0].lower() in ("默认", "官方", "reset", "default"):
            self._set_cfg("peak_windows", list(DEFAULTS["peak_windows"]))
            self._cache = None
            wins, _ = self._resolve_windows(force=True)
            return "已恢复官方时段：" + "、".join(fmt_win(w) for w in wins)
        wins = norm_windows(args)
        if not wins:
            return "时段格式不对，" + usage
        self._set_cfg("peak_windows", [fmt_win(w) for w in wins])
        self._cache = None
        return "高峰时段已设为：" + "、".join(fmt_win(w) for w in wins)

    def _cmd_holiday(self, args: list[str]) -> str:
        """节假日：官方在这些天全天算空闲时段，不拦。"""
        cur = [str(x) for x in (self._cfg("holidays", []) or [])]
        shown = "、".join(cur) or "未设置"

        def _today() -> str:
            day = self._now().date()
            peak_day, note = self._day_status(day)
            return (
                f"今天 {day.isoformat()}（{WEEK_CN[day.weekday()]}）算{note}，"
                + ("要拦。" if peak_day else "不拦。")
            )

        if not args:
            return f"节假日：{shown}\n{_today()}\n用法：/高峰 假日 加 2027-01-01~2027-01-03"

        word = args[0].lower()
        if word in ("加", "添加", "add"):
            added = parse_days(args[1:])
            if not added:
                return "日期格式不对，写成 2027-01-01 或 2027-01-01~2027-01-03"
            pool = self._holiday_set() | added
            self._set_cfg("holidays", compress_days(pool))
            self._holiday_cache = None
            return f"加了 {len(added)} 天，现在共 {len(pool)} 天。\n{_today()}"
        if word in ("清空", "清除", "reset"):
            if len(args) > 1 and args[1] in ("确认", "yes", "ok"):
                self._set_cfg("holidays", [])
                self._holiday_cache = None
                return "节假日清空了，现在只按周末判定。\n" + _today()
            return "会删掉所有节假日。确认请发：/高峰 假日 清空 确认"
        if word in ("列表", "list", "看"):
            return f"节假日：{shown}\n{_today()}"
        return "用法：/高峰 假日 [加 日期... | 清空 确认]"

    def _cmd_weekend(self, args: list[str]) -> str:
        cur = bool(self._cfg("skip_weekends", True))

        def _show() -> str:
            return (
                f"周末判定：{'跳过（周末不拦）' if cur else '照拦（周末也按高峰）'}"
                "\n用法：/高峰 周末 on|off"
            )

        if not args:
            return _show()
        word = args[0].lower()
        if word in ("on", "开", "开启", "1", "true", "跳过"):
            new = True
        elif word in ("off", "关", "关闭", "0", "false", "照拦"):
            new = False
        else:
            return "用法：/高峰 周末 on|off（on = 周末不拦）"
        self._set_cfg("skip_weekends", new)
        day = self._now().date()
        peak_day, note = self._day_status(day)
        return (
            ("周末不拦了。" if new else "周末也按高峰拦。")
            + f"\n今天 {WEEK_CN[day.weekday()]} 算{note}，"
            + ("要拦。" if peak_day else "不拦。")
        )

    def _cmd_daily(self, args: list[str]) -> str:
        def _cur() -> str:
            on = bool(self._cfg("daily_report_enabled", False))
            t = str(self._cfg("daily_report_time", "21:00"))
            tgt = str(self._cfg("daily_report_target", "") or "").strip()
            tgt = tgt or str(self._stats.get("last_admin_umo") or "未记录（先跟我说句话）")
            return f"日报：{'开' if on else '关'} · 每天 {t} · 发到 {tgt}"

        if not args:
            return _cur() + "\n用法：/高峰 日报 on|off   /高峰 日报 21:00   或在面板里改"

        word = args[0].lower()
        if word in ("on", "开", "开启", "1", "true"):
            self._set_cfg("daily_report_enabled", True)
            return _cur() + "\n开了。"
        if word in ("off", "关", "关闭", "0", "false"):
            self._set_cfg("daily_report_enabled", False)
            return "日报关了。"
        raw = word.replace("：", ":")
        try:
            hh, _, mm = raw.partition(":")
            h, m = int(hh), int(mm or 0)
            if not (0 <= h < 24 and 0 <= m < 60):
                raise ValueError
        except ValueError:
            return "时间要写成 HH:MM，比如 21:00"
        text = f"{h:02d}:{m:02d}"
        self._set_cfg("daily_report_time", text)
        self._set_cfg("daily_report_enabled", True)
        return _cur() + "\n时间改好了。"

    # ------------------------------------------------------------ 文案

    def _status_text(self) -> str:
        st = self._state()
        mode = "peak（官方峰谷）" if self._mode() == "peak" else "off（只统计）"
        head = f"【高峰守卫】{'开' if self._enabled() else '关'} · 模式 {mode}"
        if st.get("locked"):
            head += " · 🔒 锁死中"
        pieces = [head, f"现在：{self._state_line(st)}"]
        pieces.append(self._grant_line())
        if self._global_wl_enabled():
            gids = self._global_whitelist_ids()
            note = f"全局访问控制白名单：开（{len(gids)} 个会话）"
            if self._grant_mode() == "admin_only":
                note += " · 这些人高峰照样会被拦，想放开就 /高峰 放行 全局"
            pieces.append(note)
        pieces.append("高峰时段：" + ("、".join(st["windows"]) or "无") + f"（{st['source']}）")
        pieces.append(
            f"今天{WEEK_CN[st['now'].weekday()]}·{st['day_note']}"
            + ("" if st["peak_day"] else "，全天不拦")
        )
        buckets: dict = self._stats.get("buckets") or {}
        today = st["now"].strftime("%Y-%m-%d")
        today_total = sum(
            int(v) for k, v in buckets.items() if str(k).startswith(today)
        )
        days = max(1, int(self._cfg("analysis_days", 14) or 14))
        cutoff = (st["now"].date() - timedelta(days=days - 1)).isoformat()
        win_total = sum(int(v) for k, v in buckets.items() if str(k)[:10] >= cutoff)
        pieces.append(f"今日已用：{human(today_total)} token")
        pieces.append(f"近{days}天：{human(win_total)} token（{len(buckets)} 个小时桶）")
        return "\n".join(pieces)

    @staticmethod
    def _state_line(st: dict[str, Any]) -> str:
        if st.get("locked"):
            return "🔒 全天锁死，谷时也拦（只放管理员）"
        if st["blocked"]:
            return f"高峰期，{st['until']} 放开"
        if not st["peak_day"]:
            return f"不在高峰（今天算{st['day_note']}）"
        return "不在高峰"

    def _stats_text(self) -> str:
        buckets: dict = self._stats.get("buckets") or {}
        days = max(1, int(self._cfg("analysis_days", 14) or 14))
        now = self._now()
        cutoff = (now.date() - timedelta(days=days - 1)).isoformat()
        per_hour = [0] * 24
        day_set: set[str] = set()
        for key, val in buckets.items():
            text = str(key)
            day, _, hour = text.partition(" ")
            if day < cutoff:
                continue
            try:
                hi = int(hour)
                vi = int(val)
            except (ValueError, TypeError):
                continue
            if 0 <= hi < 24 and vi > 0:
                per_hour[hi] += vi
                day_set.add(day)
        if not day_set:
            return f"近{days}天还没有数据，聊几句就有了。"

        n = len(day_set)
        avg = [x / n for x in per_hour]
        total = sum(per_hour)
        mean = total / n / 24
        wins, src = self._resolve_windows(force=True)
        order = sorted(range(24), key=lambda h: -avg[h])[:8]
        lines = [f"近{days}天统计（{n} 天有数据，共 {human(total)} token）"]
        for h in order:
            if avg[h] <= 0:
                continue
            bar = "█" * max(1, round(avg[h] / (mean * 2) * 6)) if mean else ""
            mark = " ←高峰" if any(in_window(h * 60 + 1, w) for w in wins) else ""
            lines.append(f"{h:02d}点  {human(avg[h]):>7}/天  {bar}{mark}")
        lines.append("高峰时段：" + ("、".join(fmt_win(w) for w in wins) or "无") + f"（{src}）")
        return "\n".join(lines)

    def _help_text(self) -> str:
        return "\n".join(
            [
                "高峰守卫用法（管理员）· 照 DeepSeek 峰谷计费时段",
                "/高峰 状态            看现在拦不拦",
                "/高峰 统计            看各小时用量",
                "/高峰 开关 on|off     总开关",
                "/高峰 锁死 on|off     锁死模式（谷时也拦，只放管理员）",
                "/高峰 锁死 文案 xxx   锁死时回什么",
                "/高峰 放行 管理员|全局|白名单|全部   谁能说话",
                "/高峰 白名单 加|删 <ID>   名单维护（本插件自己的）",
                "/高峰 模式 peak|off   peak=按峰谷拦，off=只统计",
                "/高峰 时段 09:00-12:00 14:00-18:00   改峰谷时段",
                "/高峰 时段 默认       恢复官方时段",
                "/高峰 假日            看节假日表",
                "/高峰 假日 加 2027-01-01~2027-01-03  加节假日",
                "/高峰 周末 on|off     周末拦不拦",
                "/高峰 日报 on|off     每日用量日报",
                "/高峰 日报 21:00     改日报时间",
                "/高峰 日报 现在       立刻发一份",
                "/高峰 重算            重算时段缓存",
                "/高峰 清除 确认       清空统计",
            ]
        )

    # ------------------------------------------------------------ WebUI 面板

    def _register_web_api(self) -> None:
        """把面板要用的读写接口挂到 dashboard 上。"""
        register = getattr(self.context, "register_web_api", None)
        if not callable(register):
            logger.info("[peak_guard] 当前环境没有 register_web_api，面板跳过注册")
            return
        register(
            "/token_peak_guard/status",
            self._api_status,
            ["GET"],
            "高峰守卫状态与用量",
        )
        register(
            "/token_peak_guard/settings",
            self._api_settings,
            ["POST"],
            "更新高峰守卫设置",
        )
        logger.info("[peak_guard] 面板接口已注册")

    def _api_payload(self) -> dict[str, Any]:
        st = self._state()
        now = st["now"]
        buckets: dict = self._stats.get("buckets") or {}
        today = now.strftime("%Y-%m-%d")
        today_total = sum(
            int(v) for k, v in buckets.items() if str(k).startswith(today)
        )
        days = max(1, min(int(self._cfg("analysis_days", 14) or 14), 60))
        series = []
        for i in range(days - 1, -1, -1):
            d = (now.date() - timedelta(days=i)).isoformat()
            total = sum(int(v) for k, v in buckets.items() if str(k).startswith(d))
            series.append({"day": d[5:], "tokens": total})
        mode = self._mode()
        return {
            "enabled": self._enabled(),
            "hard_lock": self._hard_lock(),
            "lock_text": str(self._cfg("priority.hard_lock_text", "")),
            "mode": mode,
            "grant": self._grant_mode(),
            "grant_cn": self.GRANT_CN.get(self._grant_mode(), ""),
            "grant_list": [
                {"value": k, "label": v} for k, v in self.GRANT_CN.items()
            ],
            "whitelist_count": len(self._whitelist_ids()),
            "blocked": bool(st["blocked"]),
            "locked": bool(st["locked"]),
            "until": st["until"],
            "windows": st["windows"],
            "source": st["source"],
            "now": now.strftime("%H:%M"),
            "weekday": WEEK_CN[now.weekday()],
            "peak_day": bool(st["peak_day"]),
            "day_note": st["day_note"],
            "skip_weekends": bool(self._cfg("skip_weekends", True)),
            "notice_enabled": bool(self._cfg("notice_enabled", True)),
            "today_tokens": today_total,
            "today_human": human(today_total),
            "range_tokens": sum(item["tokens"] for item in series),
            "range_human": human(sum(item["tokens"] for item in series)),
            "analysis_days": days,
            "series": series,
        }

    async def _api_status(self):
        try:
            return self._api_payload()
        except Exception as e:
            logger.warning(f"[peak_guard] 面板读取状态失败: {e}")
            return {"status": "error", "message": str(e)}

    async def _api_settings(self):
        try:
            from quart import request
        except Exception as e:
            return {"status": "error", "message": f"缺少 quart：{e}"}
        try:
            payload = await request.get_json()
        except Exception:
            payload = None
        if not isinstance(payload, dict):
            return {"status": "error", "message": "请求体必须是 JSON 对象"}

        changed: list[str] = []
        try:
            for key, target in (
                ("enabled", "priority.enabled"),
                ("hard_lock", "priority.hard_lock"),
                ("notice_enabled", "notice_enabled"),
                ("skip_weekends", "skip_weekends"),
            ):
                if key in payload:
                    if not isinstance(payload[key], bool):
                        return {"status": "error", "message": f"{key} 必须是布尔值"}
                    self._set_cfg(target, payload[key])
                    changed.append(key)
            if "mode" in payload:
                mode = str(payload["mode"]).strip().lower()
                if mode not in ("peak", "off"):
                    return {"status": "error", "message": "mode 只能是 peak 或 off"}
                self._set_cfg("mode", mode)
                changed.append("mode")
            if "grant" in payload:
                grant = str(payload["grant"]).strip()
                if grant not in self.GRANT_CN:
                    return {"status": "error", "message": "不支持的放行范围"}
                self._set_cfg("priority.grant", grant)
                changed.append("grant")
            if "windows" in payload:
                wins = norm_windows(payload["windows"])
                if not wins:
                    return {
                        "status": "error",
                        "message": "时段格式不对，例：09:00-12:00 14:00-18:00",
                    }
                self._set_cfg("peak_windows", [fmt_win(w) for w in wins])
                self._cache = None
                changed.append("windows")
            if "lock_text" in payload:
                text = str(payload["lock_text"]).strip()
                if text:
                    self._set_cfg("priority.hard_lock_text", text[:200])
                    changed.append("lock_text")
        except Exception as e:
            logger.warning(f"[peak_guard] 面板保存失败: {e}")
            return {"status": "error", "message": str(e)}

        try:
            data = self._api_payload()
        except Exception as e:
            return {"status": "error", "message": str(e)}
        data["changed"] = changed
        return data
