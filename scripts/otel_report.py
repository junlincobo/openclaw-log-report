"""
openclaw session.jsonl → TelemetryAPI 上报
通过 POST /api/v1/telemetry 走后端统一通道，由后端转发到 Langfuse。
零外部依赖，只需 caw 已安装（自动读取 ~/.cobo-agentic-wallet/ 的 API key）。

上报策略: 每轮 turn 一次 POST，turn record 携带 children（llm_call + tool_call）。
  POST #1: session root span
  POST #2..N+1: turn:0 ~ turn:N（各自含 children）

span 结构（与原 OTel 版等效）:
  session:<skill>           ← 根 span
    turn:<N>                ← 每轮对话
      llm_call              ← generation (含 token 用量)
      caw:<op>              ← caw CLI 调用
      exec:<name>           ← 其他 exec
      file_read / web_search / process_poll
"""

import getpass
import json
import os
import random
import re
import glob
import socket
import string
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

DEFAULT_SESSIONS_DIR = str(Path.home() / ".openclaw" / "agents" / "main" / "sessions")


# ── caw 操作分类表（同步自 sdk/go/cmd/caw/ 源码，2026-04-04）─────────────────
CAW_OP_TABLE = [
    # Onboarding
    (["onboard bootstrap"],           "caw.onboard.bootstrap", "onboarding"),
    (["onboard health"],              "caw.onboard.health",    "onboarding"),
    (["onboard self-test"],           "caw.onboard.self_test", "onboarding"),
    (["onboard"],                     "caw.onboard",           "onboarding"),
    # Transactions
    (["tx transfer"],                 "caw.tx.transfer",       "transaction"),
    (["tx call"],                     "caw.tx.call",           "transaction"),
    (["tx sign-message"],             "caw.tx.sign_message",   "transaction"),
    (["tx speedup"],                  "caw.tx.speedup",        "transaction"),
    (["tx drop"],                     "caw.tx.drop",           "transaction"),
    (["tx estimate-transfer-fee"],    "caw.tx.estimate_fee",   "query"),
    (["tx estimate-call-fee"],        "caw.tx.estimate_call_fee", "query"),
    (["tx list"],                     "caw.tx.list",           "query"),
    (["tx get"],                      "caw.tx.get",            "query"),
    # Wallet
    (["wallet balance"],              "caw.wallet.balance",    "query"),
    (["wallet list"],                 "caw.wallet.list",       "query"),
    (["wallet get"],                  "caw.wallet.get",        "query"),
    (["wallet current"],              "caw.wallet.current",    "query"),
    (["wallet pair-status"],          "caw.wallet.pair_status","wallet"),
    (["wallet pair"],                 "caw.wallet.pair",       "wallet"),
    (["wallet rename"],               "caw.wallet.rename",     "wallet"),
    (["wallet archive"],              "caw.wallet.archive",    "wallet"),
    # Address
    (["address create"],              "caw.address.create",    "wallet"),
    (["address list"],                "caw.address.list",      "query"),
    # Status
    (["status"],                      "caw.status",            "query"),
    # Pending / Authorization
    (["pending approve"],             "caw.pending.approve",   "auth"),
    (["pending reject"],              "caw.pending.reject",    "auth"),
    (["pending list"],                "caw.pending.list",      "auth"),
    (["pending get"],                 "caw.pending.get",       "auth"),
    # Pact
    (["pact submit"],                 "caw.pact.submit",       "auth"),
    (["pact status"],                 "caw.pact.status",       "auth"),
    (["pact show"],                   "caw.pact.show",         "auth"),
    (["pact events"],                 "caw.pact.events",       "auth"),
    (["pact list"],                   "caw.pact.list",         "auth"),
    (["pact revoke"],                 "caw.pact.revoke",       "auth"),
    (["pact withdraw"],               "caw.pact.withdraw",     "auth"),
    (["pact update-conditions"],      "caw.pact.update_conditions", "auth"),
    (["pact update-policies"],        "caw.pact.update_policies",  "auth"),
    # Approval
    (["approval create"],             "caw.approval.create",   "auth"),
    (["approval resolve"],            "caw.approval.resolve",  "auth"),
    (["approval list"],               "caw.approval.list",     "auth"),
    (["approval get"],                "caw.approval.get",      "auth"),
    # AP2 / Shopping
    (["ap2 shipping delete"],         "caw.ap2.shipping_delete","ap2"),
    (["ap2 shipping list"],           "caw.ap2.shipping_list", "ap2"),
    (["ap2 shipping set"],            "caw.ap2.shipping_set",  "ap2"),
    (["ap2 shipping show"],           "caw.ap2.shipping_show", "ap2"),
    (["ap2 merchants"],               "caw.ap2.merchants",     "ap2"),
    (["ap2 purchase"],                "caw.ap2.purchase",      "ap2"),
    (["ap2 cancel"],                  "caw.ap2.cancel",        "ap2"),
    (["ap2 status"],                  "caw.ap2.status",        "ap2"),
    (["ap2 search"],                  "caw.ap2.search",        "ap2"),
    (["ap2 list"],                    "caw.ap2.list",          "ap2"),
    (["ap2"],                         "caw.ap2",               "ap2"),
    # Payment (MPP)
    (["payment session close-all"],   "caw.payment.session_close_all", "payment"),
    (["payment session close"],       "caw.payment.session_close",     "payment"),
    (["payment session list"],        "caw.payment.session_list",      "payment"),
    (["payment session withdraw"],    "caw.payment.session_withdraw",  "payment"),
    (["payment gateway"],             "caw.payment.gateway",           "payment"),
    # Track / Monitor
    (["track"],                       "caw.track",             "monitor"),
    # Node
    (["node status"],                 "caw.node.status",       "node"),
    (["node start"],                  "caw.node.start",        "node"),
    (["node stop"],                   "caw.node.stop",         "node"),
    (["node restart"],                "caw.node.restart",      "node"),
    (["node health"],                 "caw.node.health",       "node"),
    (["node info"],                   "caw.node.info",         "node"),
    (["node logs"],                   "caw.node.logs",         "node"),
    # Utilities
    (["util abi selector"],           "caw.util.abi_selector", "util"),
    (["util abi encode"],             "caw.util.abi_encode",   "util"),
    (["util abi decode"],             "caw.util.abi_decode",   "util"),
    (["util base64 encode"],          "caw.util.base64_encode","util"),
    (["util base64 decode"],          "caw.util.base64_decode","util"),
    # Meta
    (["meta chain-info"],             "caw.meta.chain_info",   "meta"),
    (["meta search-tokens"],          "caw.meta.search_tokens","meta"),
    (["meta prices"],                 "caw.meta.prices",       "meta"),
    (["meta chains"],                 "caw.meta.chains",       "meta"),
    (["meta tokens"],                 "caw.meta.tokens",       "meta"),
    # Dev / Faucet
    (["faucet deposit"],              "caw.faucet.deposit",    "dev"),
    (["faucet tokens"],               "caw.faucet.tokens",     "dev"),
    # Misc
    (["update"],                      "caw.update",            "meta"),
    (["fetch"],                       "caw.fetch",             "util"),
    (["export-key"],                  "caw.export_key",        "wallet"),
    (["demo"],                        "caw.demo",              "dev"),
    (["schema"],                      "caw.schema",            "meta"),
    (["version", "--version"],        "caw.version",           "meta"),
    (["--help", "-h"],                "caw.help",              "meta"),
]

CAW_BIN_PATTERN = re.compile(
    r"(?:^|&&\s*)"
    r"(?:[^\s]*?/)?caw\s+"
    r"(.*?)(?:\s+&&|\s*$)",
    re.MULTILINE
)
SKILL_INSTALL_PATTERN = re.compile(
    r"(?:npx\s+skills\s+add|clawhub\s+install|npx\s+skills\s+update)\s+(\S+)"
)
BOOTSTRAP_PATTERN = re.compile(r"bootstrap-env\.sh")

ONBOARD_FIELDS = ["PHASE", "BOOTSTRAP_STAGE", "WALLET_STATUS", "WALLET_UUID", "AGENT_ID"]
POLICY_DENIAL_PATTERN = re.compile(
    r"(?:TRANSFER_LIMIT_EXCEEDED|POLICY_DENIED|403|policy.*denied|suggestion[\":\s]+([^\n]+))",
    re.IGNORECASE
)
UPDATE_SIGNAL = re.compile(r'"update"\s*:\s*true')


# ── caw 配置读取 ──────────────────────────────────────────────────────────────

def _caw_config_dir() -> Path:
    return Path.home() / ".cobo-agentic-wallet"


def load_caw_config() -> dict[str, str]:
    """从 ~/.cobo-agentic-wallet/ 读取 API key/URL/agent_id 等。

    读取顺序: env vars > caw profile credentials。
    返回 {"api_key", "api_url", "agent_id", "wallet_uuid", "env"}。
    """
    result: dict[str, str] = {}

    # 从 caw profile 读
    config_path = _caw_config_dir() / "config"
    if config_path.exists():
        cfg = json.loads(config_path.read_text())
        profile_id = cfg.get("default_profile", "")
        if profile_id:
            cred_path = _caw_config_dir() / "profiles" / f"profile_{profile_id}" / "credentials"
            if cred_path.exists():
                cred = json.loads(cred_path.read_text())
                result["api_key"] = cred.get("api_key", "")
                result["api_url"] = cred.get("api_url", "")
                result["agent_id"] = cred.get("agent_id", "")
                result["wallet_uuid"] = cred.get("wallet_uuid", "")
                result["env"] = cred.get("env", "")

    # env vars 优先覆盖
    if v := os.environ.get("CAW_API_KEY"):
        result["api_key"] = v
    if v := os.environ.get("AGENT_WALLET_API_URL"):
        result["api_url"] = v

    return result


# ── JSONL 解析 ────────────────────────────────────────────────────────────────

def parse_session(path: str) -> dict:
    messages, order = {}, []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            ev = json.loads(line)
            eid = ev.get("id") or ev.get("uuid") or ev.get("type", "")
            if eid:
                messages[eid] = ev
                order.append(eid)

    session_ev = next((messages[i] for i in order if messages[i].get("type") == "session"), {})
    # OpenClaw: model-snapshot event
    snapshot = next(
        (messages[i]["data"] for i in order if messages[i].get("customType") == "model-snapshot"),
        {}
    )

    # Fallback for Claude Code format (no "type: session" event)
    if not session_ev:
        first_msg = next(
            (messages[i] for i in order if messages[i].get("type") in ("user", "assistant")),
            {}
        )
        session_id = first_msg.get("sessionId", os.path.basename(path).replace(".jsonl", ""))
        started_at = first_msg.get("timestamp")
        cwd = first_msg.get("cwd", "")
        # Claude Code: model is in the first assistant message
        first_assistant = next(
            (messages[i] for i in order
             if messages[i].get("type") == "assistant"
             and messages[i].get("message", {}).get("model")),
            {}
        )
        model_name = first_assistant.get("message", {}).get("model", "unknown")
    else:
        session_id = session_ev.get("id", os.path.basename(path).replace(".jsonl", ""))
        started_at = session_ev.get("timestamp")
        cwd = session_ev.get("cwd", "")
        model_name = snapshot.get("modelId", "unknown")

    return {
        "session_id": session_id,
        "started_at": started_at,
        "cwd": cwd,
        "model": model_name,
        "provider": snapshot.get("provider", "unknown"),
        "messages": messages,
        "order": order,
    }


def _is_message_event(ev: dict) -> bool:
    """Check if event is a user/assistant message (OpenClaw or Claude Code format)."""
    ev_type = ev.get("type", "")
    # OpenClaw: type="message"
    if ev_type == "message":
        return True
    # Claude Code: type="user" or type="assistant" with message.role
    if ev_type in ("user", "assistant") and "message" in ev:
        return True
    return False


def extract_message_events(session: dict) -> list[dict]:
    return [session["messages"][i] for i in session["order"]
            if _is_message_event(session["messages"][i])]


def build_turns(message_events: list[dict]) -> list[list[dict]]:
    """每次 role=user（非 toolResult）开启新轮次"""
    turns, current = [], []
    for ev in message_events:
        role = ev.get("message", {}).get("role")
        # Skip tool_result-only user messages (Claude Code embeds tool_result in user type)
        if role == "user":
            content = ev.get("message", {}).get("content", [])
            if isinstance(content, list):
                dict_blocks = [b for b in content if isinstance(b, dict)]
                if dict_blocks and all(b.get("type") == "tool_result" for b in dict_blocks):
                    # This is a toolResult message, don't start a new turn
                    if current:
                        current.append(ev)
                    continue
        if role == "user" and current:
            turns.append(current)
            current = []
        current.append(ev)
    if current:
        turns.append(current)
    return turns


def build_tool_result_index(message_events: list[dict]) -> dict:
    """Build index of tool results by tool call ID.

    Supports both OpenClaw (role=toolResult, toolCallId) and
    Claude Code (role=user, content[].type=tool_result, tool_use_id).
    """
    index: dict[str, dict] = {}
    for ev in message_events:
        msg = ev.get("message", {})
        # OpenClaw format
        if msg.get("role") == "toolResult" and msg.get("toolCallId"):
            index[msg["toolCallId"]] = ev
        # Claude Code format: user message with tool_result content blocks
        elif msg.get("role") == "user":
            content = msg.get("content", [])
            if isinstance(content, list):
                for block in content:
                    if isinstance(block, dict) and block.get("type") == "tool_result":
                        tool_use_id = block.get("tool_use_id", "")
                        if tool_use_id:
                            # Wrap as a compatible format
                            index[tool_use_id] = {
                                "message": {
                                    "role": "toolResult",
                                    "content": block.get("content", []),
                                    "details": {},
                                },
                                "timestamp": ev.get("timestamp"),
                            }
    return index


# ── caw 命令解析 ──────────────────────────────────────────────────────────────

def parse_caw_command(command: str) -> Optional[tuple[str, str, str]]:
    m = CAW_BIN_PATTERN.search(command)
    if not m:
        return None
    subcmd = m.group(1).strip()
    # Detect help invocations before cleaning
    if "--help" in subcmd or subcmd.endswith("-h"):
        return "caw.help", "meta", subcmd
    clean = re.sub(r"--(?:format|env|profile|timeout|verbose|api-key|api-url)\s*\S*", "", subcmd).strip()
    for prefixes, span_name, category in CAW_OP_TABLE:
        for p in prefixes:
            if clean.startswith(p):
                return span_name, category, subcmd
    return "caw.unknown", "unknown", subcmd


def extract_caw_flags(subcmd: str) -> dict:
    flags = {}
    for flag, key in [
        (r"--to\s+(\S+)",          "to_address"),
        (r"--token-id\s+(\S+)",    "token_id"),
        (r"--amount\s+(\S+)",      "amount"),
        (r"--chain\s+(\S+)",       "chain"),
        (r"--request-id\s+(\S+)",  "request_id"),
        (r"--wallet-id\s+(\S+)",   "wallet_id"),
        (r"--env\s+(\S+)",         "env"),
        (r"--contract\s+(\S+)",    "contract"),
        (r"--context\s+'([^']+)'", "context"),
    ]:
        m = re.search(flag, subcmd)
        if m:
            flags[key] = m.group(1)
    return flags


def parse_onboard_table(text: str) -> dict:
    result = {}
    lines = [l for l in text.split("\n") if l.strip()]
    header_line = next((l for l in lines if "ONBOARD_SESSION_ID" in l), None)
    if not header_line:
        return result
    headers = header_line.split()
    data_line = next((l for l in lines[lines.index(header_line)+1:]
                      if l.strip() and not l.startswith("-")), None)
    if not data_line:
        return result
    values = data_line.split()
    for i, h in enumerate(headers):
        if h in ONBOARD_FIELDS and i < len(values):
            result[h.lower()] = values[i]
    return result


def parse_tx_result(text: str) -> dict:
    result = {}
    try:
        data = json.loads(text)
        inner = data.get("result", data)
        for k in ["transaction_id", "tx_hash", "status", "request_id", "error_code", "suggestion"]:
            if k in inner:
                result[k] = str(inner[k])
        if data.get("update"):
            result["caw_update_available"] = "true"
    except Exception:
        m = POLICY_DENIAL_PATTERN.search(text)
        if m:
            result["policy_denial"] = m.group(0)[:200]
        if UPDATE_SIGNAL.search(text):
            result["caw_update_available"] = "true"
    return result


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def ts_to_ns(ts: Optional[str]) -> Optional[int]:
    if not ts:
        return None
    try:
        return int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1e9)
    except Exception:
        return None


def safe_str(obj, limit: int = 2000) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, default=str) if not isinstance(obj, str) else obj
        return s[:limit]
    except Exception:
        return str(obj)[:limit]


def extract_user_text(msg: dict) -> str:
    parts = []
    content = msg.get("content", [])
    # Handle string content (Claude Code sometimes uses plain string)
    if isinstance(content, str):
        return content[:400]
    for block in content:
        if isinstance(block, str):
            parts.append(block[:400])
            continue
        if not isinstance(block, dict) or block.get("type") != "text":
            continue
        text = block.get("text", "")
        text = re.sub(
            r"Conversation info \(untrusted metadata\):.*?(?=\n\n|\Z)",
            "", text, flags=re.DOTALL
        ).strip()
        text = re.sub(r"^System:.*", "", text, flags=re.MULTILINE).strip()
        # Remove Sender metadata block for turn name preview
        text = re.sub(
            r"Sender \(untrusted metadata\):\s*```json\s*\{.*?\}\s*```",
            "", text, flags=re.DOTALL
        ).strip()
        if text:
            parts.append(text[:400])
    return " | ".join(parts)


def extract_sender_id(msg: dict) -> str:
    """Extract user identifier from message metadata.

    Priority: sender_id (Telegram) > id (TUI/gateway) > label > empty.
    """
    for block in msg.get("content", []):
        if not isinstance(block, dict):
            continue
        text = block.get("text", "")
        # Telegram: "sender_id": "7367314769"
        m = re.search(r'"sender_id":\s*"([^"]+)"', text)
        if m:
            return m.group(1)
        # Terminal/gateway: "id": "gateway-client"
        m = re.search(r'"id":\s*"([^"]+)"', text)
        if m:
            return m.group(1)
        # Fallback: "label": "openclaw-tui (gateway-client)"
        m = re.search(r'"label":\s*"([^"]+)"', text)
        if m:
            return m.group(1)
    return ""


def extract_sender_name(msg: dict) -> str:
    for block in msg.get("content", []):
        if not isinstance(block, dict):
            continue
        m = re.search(r'"sender":\s*"([^"]+)"', block.get("text", ""))
        if m:
            return m.group(1)
    return "unknown"


# ── HTTP 上报 ─────────────────────────────────────────────────────────────────

def post_session(api_url: str, api_key: str, record: dict) -> bool:
    """POST session record 到 /api/v1/telemetry/session。返回是否成功。"""
    url = f"{api_url}/api/v1/telemetry/session"
    data = json.dumps(record, ensure_ascii=False, default=str).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "X-API-Key": api_key,
            "User-Agent": "openclaw-log-report/1.0",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status < 300
    except urllib.error.HTTPError as e:
        print(f"[WARN] POST {url} → {e.code}: {e.read()[:500].decode(errors='replace')}")
        return False
    except Exception as e:
        print(f"[WARN] POST {url} → {e}")
        return False


# ── TelemetryRecord 构造器 ────────────────────────────────────────────────────

class SessionUploader:
    """解析 session.jsonl，构造 TelemetryRecord JSON，逐 turn POST 上报。"""

    def __init__(self, api_url: str, api_key: str,
                 skill_name: str = "cobo-agentic-wallet-sandbox",
                 resource: Optional[dict[str, str]] = None):
        self.api_url = api_url.rstrip("/")
        self.api_key = api_key
        self.skill = skill_name
        self.resource = resource or {}
    def _post_session(self, record: dict) -> bool:
        return post_session(self.api_url, self.api_key, record)

    # ── 会话级 ────────────────────────────────────────────────────────────────

    def upload(self, session: dict, user_id: str = "",
               since_ts: Optional[float] = None, until_ts: Optional[float] = None) -> None:
        evts = extract_message_events(session)
        turns = build_turns(evts)
        tr_idx = build_tool_result_index(evts)

        # Filter turns by time range if specified
        if since_ts or until_ts:
            since_ns = int(since_ts * 1e9) if since_ts else 0
            until_ns = int(until_ts * 1e9) if until_ts else float("inf")
            filtered_turns = []
            for turn in turns:
                turn_start = ts_to_ns(turn[0].get("timestamp")) or 0
                turn_end = ts_to_ns(turn[-1].get("timestamp")) or turn_start
                # Keep turn if it overlaps with [since_ns, until_ns]
                if turn_start <= until_ns and turn_end >= since_ns:
                    filtered_turns.append(turn)
            skipped = len(turns) - len(filtered_turns)
            if skipped:
                print(f"[INFO] Filtered: {len(filtered_turns)} turns in range, {skipped} skipped")
            turns = filtered_turns

        sid = session["session_id"]
        model = session["model"]
        prov = session["provider"]

        first_user = next(
            (e for e in evts if e.get("message", {}).get("role") == "user"), None
        )
        if first_user and not user_id:
            user_id = extract_sender_id(first_user.get("message", {})) or "unknown"

        start_ns = ts_to_ns(session["started_at"])
        all_events = [ev for turn in turns for ev in turn]
        last_ns = ts_to_ns(all_events[-1].get("timestamp")) if all_events else start_ns

        tz_cn = timezone(offset=timedelta(hours=8))
        now_cn = datetime.now(tz=tz_cn)
        time_code = now_cn.strftime("%m%d%H%M")
        import socket
        user = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
        hostname = socket.gethostname()
        # Simplify hostname: "lujunlin-openclew-dev-v1-20260330" → "lujunlin-openclew"
        parts = hostname.split("-")
        hostname_short = "-".join(parts[:2]) if len(parts) > 2 else hostname
        trace_display_name = f"script_{user}@{hostname_short}_{time_code}"
        upload_iso = now_cn.isoformat()

        # Build all turn children
        turn_children: list[dict] = []
        for i, turn in enumerate(turns):
            turn_record = self._build_turn_record(turn, i, model, prov, tr_idx)
            turn_children.append(turn_record)

        # Single POST: SessionRecord with all turns as children
        session_record: dict = {
            "name": f"session:{sid[:8]}",
            "trace_name": trace_display_name,
            "session_id": sid,
            "user_id": user_id,
            "tags": ["openclaw"],
            "start_time_unix_nano": start_ns,
            "end_time_unix_nano": last_ns,
            "metadata": {
                "skill": self.skill,
                "model": model,
                "provider": prov,
                "cwd": session.get("cwd", ""),
                "session_id": sid,
                "telemetry_source": "script",
                "uploaded_at": upload_iso,
                "host": f"{getpass.getuser()}@{socket.gethostname()}",
            },
            "attributes": {
                "langfuse.observation.input": safe_str({
                    "session_id": sid,
                    "model": model,
                    "turns": len(turns),
                }),
            },
            "children": turn_children,
        }

        ok = self._post_session(session_record)
        total_children = sum(len(t.get('children') or []) for t in turn_children)
        status = "OK" if ok else "FAILED"
        print(f"\n{'='*60}")
        print(f"  Status:      {status}")
        print(f"  Trace Name:  {trace_display_name}")
        print(f"  Session ID:  {sid}")
        print(f"  User ID:     {user_id}")
        print(f"  Model:       {model}")
        print(f"  Turns:       {len(turn_children)}")
        print(f"  Spans:       {total_children}")
        print(f"  API:         {self.api_url}")
        print(f"{'='*60}")

    # ── 轮次级 ────────────────────────────────────────────────────────────────

    def _build_turn_record(self, turn, idx, model, provider, tr_idx) -> dict:
        user_ev = turn[0]
        user_msg = user_ev.get("message", {})
        user_text_raw = extract_user_text(user_msg)
        user_text_clean = user_text_raw
        sender = extract_sender_name(user_msg)
        turn_start_ns = ts_to_ns(user_ev.get("timestamp"))
        turn_end_ns = ts_to_ns(turn[-1].get("timestamp")) if turn else turn_start_ns

        # 收集 children
        events_after_user = turn[1:]
        children: list[dict] = []
        final_text = ""
        for j, ev in enumerate(events_after_user):
            msg = ev.get("message", {})
            role = msg.get("role")
            if role == "assistant":
                # next event timestamp for LLM end_time fallback
                next_ts = None
                if j + 1 < len(events_after_user):
                    next_ts = ts_to_ns(events_after_user[j + 1].get("timestamp"))
                llm_children = self._build_assistant_children(ev, model, provider, tr_idx, next_ts)
                children.extend(llm_children)
                for b in msg.get("content", []):
                    if b.get("type") == "text":
                        final_text = b.get("text", "")[:500]

        # Turn name with cleaned user input preview (max 10 chars)
        input_preview = user_text_clean[:10].rstrip() + ".." if len(user_text_clean) > 10 else user_text_clean
        turn_name = f'turn:{idx} ("{input_preview}")' if input_preview else f"turn:{idx}"

        return {
            "name": turn_name,
            "record_type": "span",
            "start_time_unix_nano": turn_start_ns,
            "end_time_unix_nano": turn_end_ns,
            "attributes": {
                "langfuse.observation.input": safe_str({"role": "user", "content": user_text_raw}),
                "langfuse.observation.output": safe_str({"role": "assistant", "content": final_text}) if final_text else None,
                "langfuse.trace.metadata.turn_index": str(idx),
                "langfuse.trace.metadata.sender": sender,
            },
            "children": children if children else None,
        }

    # ── LLM 调用 + Tool Call → children ──────────────────────────────────────

    def _build_assistant_children(self, ev, model, provider, tr_idx, next_ev_ts: Optional[int] = None) -> list[dict]:
        children: list[dict] = []
        msg = ev.get("message", {})
        content = msg.get("content", [])
        usage = msg.get("usage", {})
        ts_ns = ts_to_ns(ev.get("timestamp"))

        tool_calls = [b for b in content if b.get("type") in ("toolCall", "tool_use")]

        # LLM timing: message.timestamp (epoch ms) = request start, ev.timestamp = response received
        msg_ts = msg.get("timestamp")
        if msg_ts and ts_ns:
            llm_start = int(msg_ts * 1e6) if isinstance(msg_ts, (int, float)) else ts_ns
            llm_end = ts_ns
        else:
            llm_start = ts_ns
            llm_end = next_ev_ts or ts_ns

        # LLM generation child
        children.append({
            "name": "OpenAI-generation",
            "record_type": "generation",
            "status_code": "OK",
            "start_time_unix_nano": llm_start,
            "end_time_unix_nano": llm_end,
            "attributes": {
                "gen_ai.request.model": msg.get("model", model),
                "langfuse.observation.model.name": msg.get("model", model),
                "gen_ai.usage.input_tokens": usage.get("input", 0),
                "gen_ai.usage.output_tokens": usage.get("output", 0),
                "langfuse.observation.output": safe_str(
                    [b.get("name") or b.get("text", "")[:80] for b in content[:5]]
                ),
                "langfuse.trace.metadata.provider": provider,
                "langfuse.trace.metadata.api": msg.get("api", ""),
                "langfuse.trace.metadata.stop_reason": msg.get("stopReason", ""),
                "langfuse.trace.metadata.response_id": msg.get("responseId", ""),
                "langfuse.observation.metadata.tool_calls_count": str(len(tool_calls)),
            },
        })

        # Tool call children
        for tc in tool_calls:
            child = self._build_tool_child(tc, tr_idx, ts_ns)
            if child:
                children.append(child)

        return children

    # ── Tool Call child ───────────────────────────────────────────────────────

    def _build_tool_child(self, tc: dict, tr_idx: dict, fallback_ts_ns) -> Optional[dict]:
        call_id = tc.get("id", "")
        name = tc.get("name", "")
        # OpenClaw: "arguments", Claude Code: "input"
        args = tc.get("arguments") or tc.get("input") or {}

        result_ev = tr_idx.get(call_id)
        result_msg = result_ev.get("message", {}) if result_ev else {}
        details = result_msg.get("details", {})
        result_ts_ns = ts_to_ns(result_ev.get("timestamp")) if result_ev else fallback_ts_ns
        dur_ms = details.get("durationMs", 0)
        if not dur_ms and fallback_ts_ns and result_ts_ns and result_ts_ns > fallback_ts_ns:
            dur_ms = int((result_ts_ns - fallback_ts_ns) / 1e6)
        ts_ns = fallback_ts_ns or result_ts_ns
        exit_code = details.get("exitCode")
        status_ok = exit_code is None or exit_code == 0

        result_text = ""
        result_content = result_msg.get("content", [])
        if isinstance(result_content, list):
            for b in result_content:
                if isinstance(b, dict) and b.get("type") == "text":
                    result_text = b.get("text", "")
                    break
                elif isinstance(b, str):
                    result_text = b
                    break
        elif isinstance(result_content, str):
            result_text = result_content

        # caw 调用 — handle both OpenClaw (exec) and Claude Code (Bash) tool names
        name_lower = name.lower()
        if name_lower in ("exec", "bash"):
            cmd = args.get("command", "")
            caw_info = parse_caw_command(cmd)
            if caw_info:
                span_name, category, subcmd = caw_info
                return self._build_caw_child(
                    span_name, category, subcmd, result_text,
                    dur_ms, ts_ns, result_ts_ns, status_ok, exit_code
                )
            if SKILL_INSTALL_PATTERN.search(cmd):
                category = "skill_install"
            elif BOOTSTRAP_PATTERN.search(cmd):
                category = "env_bootstrap"
            else:
                category = "exec"
        elif name_lower in ("read", "grep", "glob"):
            category = "file_read"
        elif name_lower in ("web_search", "webfetch", "websearch"):
            category = "web_search"
        elif name_lower in ("edit", "write", "notebookedit"):
            category = "file_write"
        elif name_lower == "process":
            category = "process_poll"
        else:
            category = name

        attrs: dict = {
            "langfuse.observation.input": safe_str(args, 1000),
            "langfuse.observation.output": result_text[:1000],
            "langfuse.observation.metadata.tool_call_id": call_id,
            "langfuse.observation.metadata.tool_name": name,
            "langfuse.observation.metadata.category": category,
            "langfuse.observation.metadata.duration_ms": str(dur_ms),
            "langfuse.observation.metadata.exit_code": str(exit_code),
        }
        if category == "skill_install":
            m = SKILL_INSTALL_PATTERN.search(args.get("command", ""))
            if m:
                attrs["langfuse.trace.metadata.skill_package"] = m.group(1)

        end_ns = result_ts_ns or (ts_ns + int(dur_ms * 1e6) if ts_ns and dur_ms else ts_ns)
        return {
            "name": f"{category}:{name}",
            "record_type": "span",
            "start_time_unix_nano": ts_ns,
            "end_time_unix_nano": end_ns,
            "status_code": "OK" if status_ok else "ERROR",
            "status_message": "" if status_ok else result_text[:200],
            "attributes": attrs,
        }

    # ── caw span ──────────────────────────────────────────────────────────────

    def _build_caw_child(self, span_name, category, subcmd, result_text,
                          dur_ms, ts_ns, result_ts_ns, status_ok, exit_code) -> dict:
        flags = extract_caw_flags(subcmd)

        attrs: dict = {
            "langfuse.observation.input": safe_str({"subcmd": subcmd[:300]}),
            "langfuse.observation.output": result_text[:1000],
            "langfuse.observation.metadata.caw_op": span_name,
            "langfuse.observation.metadata.category": category,
            "langfuse.observation.metadata.duration_ms": str(dur_ms),
            "langfuse.observation.metadata.exit_code": str(exit_code),
            "langfuse.trace.metadata.caw_op": span_name,
            "langfuse.trace.metadata.caw_category": category,
        }
        for k, v in flags.items():
            attrs[f"langfuse.trace.metadata.caw_{k}"] = v

        if "onboard" in span_name:
            parsed = parse_onboard_table(result_text)
            for k, v in parsed.items():
                if v and v not in ("-", ""):
                    attrs[f"langfuse.trace.metadata.onboard_{k}"] = v

        if category == "transaction":
            tx_fields = parse_tx_result(result_text)
            for k, v in tx_fields.items():
                attrs[f"langfuse.trace.metadata.tx_{k}"] = v
            if "policy_denial" in tx_fields or not status_ok:
                attrs["langfuse.observation.level"] = "WARNING"
                attrs["langfuse.observation.metadata.policy_denied"] = "true"

        if UPDATE_SIGNAL.search(result_text):
            attrs["langfuse.trace.metadata.caw_update_available"] = "true"

        if "context" in flags:
            try:
                ctx = json.loads(flags["context"])
                attrs["langfuse.trace.metadata.openclaw_channel"] = ctx.get("channel", "")
                attrs["langfuse.trace.metadata.openclaw_target"] = ctx.get("target", "")
            except Exception:
                pass

        status = "OK"
        if not status_ok and category not in ("query", "meta", "dev"):
            status = "ERROR"

        end_ns = result_ts_ns or (ts_ns + int(dur_ms * 1e6) if ts_ns and dur_ms else ts_ns)
        return {
            "name": span_name,
            "record_type": "span",
            "start_time_unix_nano": ts_ns,
            "end_time_unix_nano": end_ns,
            "status_code": status,
            "status_message": "" if status == "OK" else result_text[:200],
            "attributes": attrs,
        }


# ── 入口函数 ──────────────────────────────────────────────────────────────────

def upload_session_file(
    jsonl_path: str,
    api_url: str = "",
    api_key: str = "",
    user_id: str = "",
    skill_name: str = "cobo-agentic-wallet-sandbox",
    since_ts: Optional[float] = None,
    until_ts: Optional[float] = None,
) -> None:
    """上报 session 到 TelemetryAPI。自动从 caw 配置读取 API key。"""
    caw_cfg = load_caw_config()

    api_url = api_url or caw_cfg.get("api_url", "") or "https://api-core.agenticwallet.sandbox.cobo.com"
    api_key = api_key or caw_cfg.get("api_key", "") or ""
    if not api_url:
        print("[ERROR] 缺少 api_url。请设置 AGENT_WALLET_API_URL 环境变量。")
        sys.exit(1)

    resource = {
        "caw.agent_id": caw_cfg.get("agent_id", ""),
        "caw.wallet_id": caw_cfg.get("wallet_uuid", ""),
        "deployment.environment": caw_cfg.get("env", ""),
        "server.address": api_url,
    }

    session = parse_session(jsonl_path)
    print(f"[INFO] Parsing {session['session_id']}  model={session['model']}  "
          f"events={len(extract_message_events(session))}")

    uploader = SessionUploader(api_url, api_key, skill_name, resource)
    uploader.upload(session, user_id=user_id, since_ts=since_ts, until_ts=until_ts)


def watch_and_upload(
    agents_dir: str = DEFAULT_SESSIONS_DIR,
    user_id: str = "",
    skill_name: str = "cobo-agentic-wallet-sandbox",
    poll_interval_s: int = 30,
) -> None:
    """持续监听新 session，自动上报。"""
    caw_cfg = load_caw_config()
    api_url = caw_cfg.get("api_url", "")
    api_key = caw_cfg.get("api_key", "")
    if not api_url or not api_key:
        print("[ERROR] 缺少 api_url 或 api_key")
        sys.exit(1)

    resource = {
        "caw.agent_id": caw_cfg.get("agent_id", ""),
        "caw.wallet_id": caw_cfg.get("wallet_uuid", ""),
        "deployment.environment": caw_cfg.get("env", ""),
        "server.address": api_url,
    }

    uploaded: set[str] = set()
    print(f"Watching {agents_dir} ...")
    while True:
        for f in glob.glob(os.path.join(agents_dir, "*.jsonl")):
            if f.endswith(".lock"):
                continue
            key = f"{f}:{int(os.path.getmtime(f))}"
            if key in uploaded:
                continue
            try:
                session = parse_session(f)
                uploader = SessionUploader(api_url, api_key, skill_name, resource)
                uploader.upload(session, user_id=user_id)
                uploaded.add(key)
                print(f"  + {os.path.basename(f)}")
            except Exception as e:
                print(f"  x {os.path.basename(f)}: {e}")
        time.sleep(poll_interval_s)


# ── Dry Run（不上报，打印 span 树）───────────────────────────────────────────

def dry_run_session(jsonl_path: str, since_ts: Optional[float] = None,
                    until_ts: Optional[float] = None) -> None:
    session = parse_session(jsonl_path)
    evts = extract_message_events(session)
    turns = build_turns(evts)
    tr_idx = build_tool_result_index(evts)

    # Filter turns by time range
    if since_ts or until_ts:
        since_ns = int(since_ts * 1e9) if since_ts else 0
        until_ns = int(until_ts * 1e9) if until_ts else float("inf")
        original = len(turns)
        turns = [t for t in turns
                 if (ts_to_ns(t[0].get("timestamp")) or 0) <= until_ns
                 and (ts_to_ns(t[-1].get("timestamp")) or 0) >= since_ns]
        if original != len(turns):
            print(f"[INFO] Filtered: {len(turns)} turns in range, {original - len(turns)} skipped")

    import socket
    tz_cn = timezone(offset=timedelta(hours=8))
    time_code = datetime.now(tz=tz_cn).strftime("%m%d%H%M")
    user = os.environ.get("USER") or os.environ.get("USERNAME") or "unknown"
    hostname = socket.gethostname()
    parts = hostname.split("-")
    hostname_short = "-".join(parts[:2]) if len(parts) > 2 else hostname
    trace_name = f"script_{user}@{hostname_short}_{time_code}"

    print(f"{'='*60}")
    print(f"Trace:   {trace_name}")
    print(f"Session: {session['session_id']}")
    print(f"Model:   {session['model']}")
    print(f"Started: {session['started_at']}")
    print(f"Turns:   {len(turns)}")
    print(f"Events:  {len(evts)}")
    print(f"{'='*60}\n")

    for i, turn in enumerate(turns):
        user_ev = turn[0]
        user_text = extract_user_text(user_ev.get("message", {}))
        ts = user_ev.get("timestamp", "?")
        last_ts = turn[-1].get("timestamp", "?")
        print(f"[turn:{i}]  [{ts} -> {last_ts}]")
        print(f"  user: {user_text[:80]}")

        for ev in turn[1:]:
            msg = ev.get("message", {})
            role = msg.get("role")
            ev_ts = ev.get("timestamp", "")

            if role == "assistant":
                content = msg.get("content", [])
                tool_calls = [b for b in content if b.get("type") in ("toolCall", "tool_use")]
                texts = [b.get("text", "")[:60] for b in content if isinstance(b, dict) and b.get("type") == "text" and b.get("text")]
                usage = msg.get("usage", {})
                tokens = usage.get("input", 0) + usage.get("output", 0)

                msg_internal_ts = msg.get("timestamp")
                if msg_internal_ts and ev_ts:
                    try:
                        ev_epoch_ms = datetime.fromisoformat(ev_ts.replace("Z", "+00:00")).timestamp() * 1000
                        llm_dur_ms = ev_epoch_ms - msg_internal_ts
                        llm_dur_str = f"{llm_dur_ms:.0f}ms" if llm_dur_ms > 0 else "~0ms"
                    except Exception:
                        llm_dur_str = "?ms"
                else:
                    llm_dur_str = "?ms"

                print(f"  +- OpenAI-generation  [{ev_ts}]  tokens={tokens}  latency={llm_dur_str}")
                if texts:
                    print(f"  |  text: {texts[0]}")

                for tc in tool_calls:
                    call_id = tc.get("id", "")
                    name = tc.get("name", "")
                    args = tc.get("arguments", {})
                    cmd = args.get("command", "") or args.get("action", "") or args.get("path", "")

                    result_ev = tr_idx.get(call_id)
                    result_msg = result_ev.get("message", {}) if result_ev else {}
                    details = result_msg.get("details", {})
                    dur_ms = details.get("durationMs", 0)
                    exit_code = details.get("exitCode")
                    result_ts = result_ev.get("timestamp", "") if result_ev else ""
                    if not dur_ms and ev_ts and result_ts:
                        try:
                            _start = datetime.fromisoformat(ev_ts.replace("Z", "+00:00")).timestamp() * 1000
                            _end = datetime.fromisoformat(result_ts.replace("Z", "+00:00")).timestamp() * 1000
                            dur_ms = int(_end - _start)
                        except Exception:
                            pass

                    if name == "exec":
                        caw_info = parse_caw_command(cmd)
                        if caw_info:
                            span_name, category, _ = caw_info
                        elif SKILL_INSTALL_PATTERN.search(cmd):
                            span_name, category = "skill_install", "skill_install"
                        elif BOOTSTRAP_PATTERN.search(cmd):
                            span_name, category = "env_bootstrap", "env_bootstrap"
                        else:
                            span_name, category = f"exec:{name}", "exec"
                    elif name == "read":
                        span_name, category = f"file_read:{name}", "file_read"
                    elif name == "process":
                        span_name, category = "process_poll", "process_poll"
                    else:
                        span_name, category = name, name

                    status = "OK" if (exit_code is None or exit_code == 0) else f"ERR(exit={exit_code})"
                    dur_str = f"{dur_ms}ms" if dur_ms else "?ms"
                    cmd_short = _shorten(cmd, 80)

                    result_text = ""
                    for b in result_msg.get("content", []):
                        if isinstance(b, str):
                            result_text = b[:60]
                            break
                        if isinstance(b, dict) and b.get("type") == "text":
                            result_text = b.get("text", "")[:60]
                            break

                    print(f"  +- {span_name}  [{result_ts}]  {status}  {dur_str}")
                    print(f"  |  cmd: {cmd_short}")
                    if result_text:
                        print(f"  |  out: {result_text}")

        print()

    total_children = sum(
        sum(1 for e in t[1:] if e.get("message", {}).get("role") == "assistant")
        for t in turns
    )
    print(f"{'='*60}")
    print(f"POST count: 1 (session) + {len(turns)} (turns) = {1 + len(turns)}")
    print(f"Total children (llm+tool): ~{total_children}+")


def _shorten(s: str, n: int) -> str:
    s = re.sub(r'export\s+PATH=[^\s]+\s*&&\s*', '', s).strip()
    s = re.sub(r'/home/[^/]+/\.cobo-agentic-wallet/bin/', '', s)
    return s[:n] + "..." if len(s) > n else s


# ── CLI 入口 ──────────────────────────────────────────────────────────────────

def dump_session(jsonl_path: str) -> None:
    """打印完整 SessionRecord JSON（即实际 POST 的内容），用于调试。"""
    session = parse_session(jsonl_path)
    evts = extract_message_events(session)
    turns = build_turns(evts)
    tr_idx = build_tool_result_index(evts)

    uploader = SessionUploader("http://dummy", "dummy")
    sid = session["session_id"]
    now_cn = datetime.now(tz=timezone(offset=timedelta(hours=8)))
    time_code = now_cn.strftime("%Y%m%d%H%M")
    rand_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=4))
    upload_iso = now_cn.isoformat()

    # Build the same record as upload() would
    turn_children = []
    for i, turn in enumerate(turns):
        turn_children.append(uploader._build_turn_record(turn, i, session["model"], session["provider"], tr_idx))

    record: dict = {
        "name": f"session:{sid[:8]}",
        "trace_name": f"script_{time_code}_{rand_suffix}",
        "session_id": sid,
        "user_id": "dump-mode",
        "tags": ["openclaw"],
        "metadata": {"model": session["model"], "turns": len(turns), "uploaded_at": upload_iso, "host": f"{getpass.getuser()}@{socket.gethostname()}"},
        "children": turn_children,
    }

    # Print first turn with children for quick inspection
    first_with_children = next((t for t in turn_children if t.get("children")), None)
    if first_with_children:
        idx = turn_children.index(first_with_children)
        print(f"=== TURN:{idx} ({len(first_with_children.get('children', []))} children) ===")
        print(json.dumps(first_with_children, indent=2, ensure_ascii=False, default=str))

    print(f"\n=== FULL SESSION ({len(turn_children)} turns, "
          f"{sum(len(t.get('children') or []) for t in turn_children)} total children) ===")
    print(f"POST body size: {len(json.dumps(record, default=str)):,} bytes")


def _parse_time_arg(value: str) -> float:
    """Parse a time argument into a Unix timestamp.

    Supports:
      - Relative: "2h", "30m", "1d" (hours/minutes/days ago)
      - Date: "2026-04-05" (start of day, UTC+8)
      - Datetime: "2026-04-05T10:00", "2026-04-05T10:00:00"
    """
    # Relative: 2h, 30m, 1d
    m = re.match(r"^(\d+)([hmd])$", value)
    if m:
        amount, unit = int(m.group(1)), m.group(2)
        seconds = {"h": 3600, "m": 60, "d": 86400}[unit]
        return time.time() - amount * seconds

    # Absolute datetime/date (treat as UTC+8)
    tz_cn = timezone(offset=timedelta(hours=8))
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(value, fmt).replace(tzinfo=tz_cn)
            return dt.timestamp()
        except ValueError:
            continue

    print(f"[ERROR] 无法解析时间参数: {value}")
    print("  支持格式: 2h, 30m, 1d, 2026-04-05, 2026-04-05T10:00")
    sys.exit(1)


def _extract_named_arg(cli_args: list[str], name: str) -> str:
    """Extract a named argument (--name value) from cli_args, mutating the list."""
    for i, arg in enumerate(cli_args):
        if arg == name and i + 1 < len(cli_args):
            value = cli_args[i + 1]
            cli_args.pop(i + 1)
            cli_args.pop(i)
            return value
    return ""


def _get_session_time_range(path: str) -> tuple[Optional[float], Optional[float]]:
    """Read session start and end timestamps from jsonl file.

    Returns (start_ts, end_ts) as Unix timestamps.
    start = session event timestamp, end = last event timestamp.
    """
    start_ts: Optional[float] = None
    end_ts: Optional[float] = None
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                ev = json.loads(line)
                ts_str = ev.get("timestamp")
                if not ts_str:
                    continue
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
                except (ValueError, AttributeError):
                    continue
                if ev.get("type") == "session" and start_ts is None:
                    start_ts = ts
                end_ts = ts  # last event timestamp
    except Exception:
        pass
    return start_ts, end_ts


def _session_overlaps(path: str, since_ts: float, until_ts: float) -> bool:
    """Check if a session's time range overlaps with [since_ts, until_ts]."""
    start_ts, end_ts = _get_session_time_range(path)
    if start_ts is None:
        return False
    end_ts = end_ts or start_ts
    # Overlap: session starts before until AND ends after since
    return start_ts <= until_ts and end_ts >= since_ts


def _is_active_session(path: str) -> bool:
    """Check if a session file is still being written (has a .lock file)."""
    lock_path = path + ".lock"
    return os.path.exists(lock_path)


def _collect_paths_by_time(since_ts: float, until_ts: float, sessions_dir: str) -> list[str]:
    """Collect .jsonl files whose session time range overlaps [since_ts, until_ts]."""
    if not os.path.isdir(sessions_dir):
        print(f"[ERROR] 目录不存在: {sessions_dir}")
        return []
    paths = []
    for f in glob.glob(os.path.join(sessions_dir, "*.jsonl")):
        if f.endswith(".lock"):
            continue
        if _session_overlaps(f, since_ts, until_ts):
            paths.append(f)
    return sorted(paths, key=lambda f: _get_session_time_range(f)[0] or 0)


if __name__ == "__main__":
    cli_args = sys.argv[1:]
    dry = "--dry-run" in cli_args
    if dry:
        cli_args.remove("--dry-run")
    dump = "--dump" in cli_args
    if dump:
        cli_args.remove("--dump")
    watch = "--watch" in cli_args
    if watch:
        cli_args.remove("--watch")
    since_str = _extract_named_arg(cli_args, "--since")
    until_str = _extract_named_arg(cli_args, "--until")

    if watch:
        watch_dir = cli_args[0] if cli_args else DEFAULT_SESSIONS_DIR
        watch_and_upload(agents_dir=watch_dir)
    else:
        paths: list[str] = []

        # Time-range filter: --since / --until
        if since_str:
            since_ts = _parse_time_arg(since_str)
            until_ts = _parse_time_arg(until_str) if until_str else time.time()

            # Collect files: args can be files, directories, or globs
            # Sessions are matched if their time range overlaps [since, until].
            scan_dirs: list[str] = []
            for arg in cli_args:
                if os.path.isfile(arg) and arg.endswith(".jsonl"):
                    if _session_overlaps(arg, since_ts, until_ts):
                        paths.append(arg)
                elif os.path.isdir(arg):
                    scan_dirs.append(arg)
                else:
                    expanded = glob.glob(arg)
                    for f in expanded:
                        if os.path.isdir(f):
                            scan_dirs.append(f)
                        elif f.endswith(".jsonl") and not f.endswith(".lock"):
                            if _session_overlaps(f, since_ts, until_ts):
                                paths.append(f)

            # No explicit paths/dirs → use default directory
            if not paths and not scan_dirs and not cli_args:
                scan_dirs = [DEFAULT_SESSIONS_DIR]

            # Scan directories
            for d in scan_dirs:
                paths.extend(_collect_paths_by_time(since_ts, until_ts, d))

            tz_cn = timezone(offset=timedelta(hours=8))
            since_dt = datetime.fromtimestamp(since_ts, tz=tz_cn).strftime("%Y-%m-%d %H:%M")
            until_dt = datetime.fromtimestamp(until_ts, tz=tz_cn).strftime("%Y-%m-%d %H:%M")
            sources = ", ".join(scan_dirs) if scan_dirs else "files"
            print(f"[INFO] Time range: {since_dt} ~ {until_dt}  source={sources}")
            print(f"[INFO] Found {len(paths)} session(s)")
        else:
            # Expand globs and collect all paths
            for arg in cli_args:
                expanded = glob.glob(arg)
                if expanded:
                    paths.extend(f for f in expanded
                                 if f.endswith(".jsonl") and not f.endswith(".lock"))
                elif arg.endswith(".jsonl"):
                    paths.append(arg)

            # No args → upload latest session
            if not paths:
                latest = max(
                    (f for f in glob.glob(os.path.join(DEFAULT_SESSIONS_DIR, "*.jsonl"))
                     if not f.endswith(".lock")),
                    key=os.path.getmtime, default=None
                )
                if latest:
                    if _is_active_session(latest):
                        print("[WARN] Session is still active (has .lock file). Uploading snapshot.")
                    paths = [latest]

        if not paths:
            if since_str:
                print("[INFO] No sessions matched the time range.")
            else:
                print("[ERROR] 找不到 session 文件")
            sys.exit(0 if since_str else 1)

        api_url = os.environ.get("AGENT_WALLET_API_URL", "https://api-core.agenticwallet.sandbox.cobo.com")
        api_key = os.environ.get("CAW_API_KEY", "")
        user_id = os.environ.get("USER_ID", "")

        for idx, path in enumerate(sorted(paths)):
            if len(paths) > 1:
                print(f"\n[{idx + 1}/{len(paths)}] {os.path.basename(path)}")
            if dump:
                dump_session(path)
            elif dry:
                dry_run_session(path,
                                since_ts=since_ts if since_str else None,
                                until_ts=until_ts if since_str else None)
            else:
                upload_session_file(
                    path,
                    api_url=api_url,
                    api_key=api_key,
                    user_id=user_id,
                    since_ts=since_ts if since_str else None,
                    until_ts=until_ts if since_str else None,
                )
