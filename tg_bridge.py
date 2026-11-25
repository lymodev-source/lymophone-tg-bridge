#!/usr/bin/env python3
"""
OrbitalOS Cloud Multi-User Telegram Bridge Server for LymoPhone Proto-1.
Stateless StringSession architecture ready for Render Free Tier (no credit card),
VPS, or local PC. Each LymoPhone keeps its own session string in Flash NVS and
sends it via X-TG-Session; login flow (Phone -> Code -> 2FA Cloud Password)
survives cloud sleep/restarts without losing user accounts.
"""

import asyncio
import io
import json
import os
import socket
import sys
import time
from collections import deque
from pathlib import Path

from aiohttp import web
from PIL import Image
from telethon import TelegramClient, events, utils
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    PhoneNumberInvalidError,
    SessionPasswordNeededError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.account import GetPasswordRequest
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DEFAULT_PORT = int(os.environ.get("PORT", os.environ.get("TG_BRIDGE_PORT", "8088")))

API_ID: int = 0
API_HASH: str = ""

# Active authenticated Telethon clients cached by StringSession token
# token -> (TelegramClient, last_used_monotonic)
clients_cache: dict[str, tuple[TelegramClient, float]] = {}
cache_lock = asyncio.Lock()

# Pending interactive login flows keyed by X-Device-Id
# dev_id -> {"client": TelegramClient, "phone": str, "hash": str, "need_2fa": bool, "hint": str, "ts": float}
pending_flows: dict[str, dict] = {}
flow_lock = asyncio.Lock()
session_locks: dict[str, asyncio.Lock] = {}

MAX_CACHED_CLIENTS = 25
CLIENT_IDLE_SEC = 600.0
FLOW_TTL_SEC = 900.0
MAX_MEDIA_BYTES = 4 * 1024 * 1024
MAX_UPLOAD_PHOTO_BYTES = 1024 * 1024

# Fast per-session peer cache & live incoming event queues (0 ms /updates & peer lookup)
peer_cache: dict[str, dict[str, object]] = {}
inbox_queues: dict[str, deque] = {}
session_max_id: dict[str, int] = {}
last_dialog_sync: dict[str, float] = {}
attached_clients: set[int] = set()


def get_session_lock(sess: str) -> asyncio.Lock:
    lk = session_locks.get(sess)
    if lk is None:
        if len(session_locks) > 50:
            session_locks.clear()
        lk = asyncio.Lock()
        session_locks[sess] = lk
    return lk


def json_resp(payload: dict, status: int = 200) -> web.Response:
    return web.Response(
        text=json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        status=status,
        content_type="application/json",
        charset="utf-8",
    )


def get_lan_ip() -> str:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8", 80))
        return s.getsockname()[0]
    except OSError:
        return "127.0.0.1"
    finally:
        s.close()


def load_config() -> tuple[int, str, int]:
    env_id = os.environ.get("TG_API_ID", "").strip()
    env_hash = os.environ.get("TG_API_HASH", "").strip()
    cfg = {}
    if CONFIG_PATH.exists():
        try:
            cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
        except Exception:
            cfg = {}

    api_id_raw = env_id or str(cfg.get("api_id", "")).strip()
    api_hash = env_hash or str(cfg.get("api_hash", "")).strip()
    port = int(os.environ.get("PORT", os.environ.get("TG_BRIDGE_PORT", cfg.get("port", DEFAULT_PORT))))

    in_cloud = bool(os.environ.get("PORT") or os.environ.get("RENDER"))
    if (not api_id_raw.isdigit() or len(api_hash) < 16) and not in_cloud and sys.stdin.isatty():
        print("\n=== Настройка OrbitalOS Telegram Bridge ===")
        print("1. Открой https://my.telegram.org и войди под своим номером.")
        print("2. Перейди в 'API development tools' и создай приложение.")
        print("3. Вставь ниже полученные api_id и api_hash:\n")
        while not api_id_raw.isdigit():
            api_id_raw = input("Введи API_ID (число): ").strip()
        while len(api_hash) < 16:
            api_hash = input("Введи API_HASH (строка): ").strip()
        CONFIG_PATH.write_text(
            json.dumps({"api_id": int(api_id_raw), "api_hash": api_hash, "port": port}, indent=2),
            encoding="utf-8",
        )
        print(f"Сохранено в {CONFIG_PATH}\n")

    api_id = int(api_id_raw) if api_id_raw.isdigit() else 0
    return api_id, api_hash, port


def api_configured() -> bool:
    return API_ID > 0 and len(API_HASH) >= 16


def clean_text(val: str | None, max_len: int) -> str:
    if not val:
        return ""
    s = " ".join(val.replace("\r", " ").split())
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def entity_display_name(ent) -> str:
    if ent is None:
        return "Chat"
    title = getattr(ent, "title", None)
    if title:
        return clean_text(title, 36)
    first = getattr(ent, "first_name", None) or ""
    last = getattr(ent, "last_name", None) or ""
    full = (first + " " + last).strip()
    if full:
        return clean_text(full, 36)
    uname = getattr(ent, "username", None)
    if uname:
        return clean_text("@" + uname, 36)
    return str(getattr(ent, "id", "Chat"))


def get_device_id(request: web.Request) -> str:
    dev = request.headers.get("X-Device-Id", "").strip()
    return dev if dev else "default"


def get_session_hdr(request: web.Request) -> str:
    return request.headers.get("X-TG-Session", "").strip()


def clear_session_state(sess: str):
    if not sess:
        return
    peer_cache.pop(sess, None)
    inbox_queues.pop(sess, None)
    session_max_id.pop(sess, None)
    last_dialog_sync.pop(sess, None)


def cache_dialog_peer(sess: str, chat_id: str, uname: str, input_ent):
    if not sess or input_ent is None:
        return
    pmap = peer_cache.setdefault(sess, {})
    if len(pmap) > 150:
        pmap.clear()
    if chat_id:
        pmap[str(chat_id)] = input_ent
    clean_u = str(uname or "").strip().lstrip("@").lower()
    if clean_u:
        pmap[clean_u] = input_ent


async def safe_disconnect(cli: TelegramClient | None):
    if cli is not None:
        attached_clients.discard(id(cli))
        try:
            await cli.disconnect()
        except Exception:
            pass


def ensure_live_listener(sess: str, cli: TelegramClient):
    if not sess or cli is None or id(cli) in attached_clients:
        return
    attached_clients.add(id(cli))
    inbox_queues.setdefault(sess, deque(maxlen=24))

    @cli.on(events.NewMessage(incoming=True))
    async def _on_new_msg(event):
        try:
            msg = event.message
            if not msg or msg.out:
                return
            mid = int(msg.id)
            cid = str(event.chat_id if event.chat_id is not None else utils.get_peer_id(msg.peer_id))
            ent = None
            try:
                ent = await event.get_chat()
            except Exception:
                ent = None
            if ent is None:
                try:
                    ent = await event.get_sender()
                except Exception:
                    ent = None
            try:
                in_chat = await event.get_input_chat()
            except Exception:
                in_chat = None
            uname = getattr(ent, "username", "") or "" if ent is not None else ""
            cache_dialog_peer(sess, cid, uname, in_chat)
            sender = entity_display_name(ent) if ent is not None else cid
            txt, has_photo, too_big = msg_summary_ext(msg)
            q = inbox_queues.setdefault(sess, deque(maxlen=24))
            if not any(x["id"] == mid and x["chat_id"] == cid for x in q):
                q.append(
                    {
                        "id": mid,
                        "chat_id": cid,
                        "sender": sender,
                        "username": uname,
                        "text": txt,
                        "photo": bool(has_photo),
                        "too_big": bool(too_big),
                    }
                )
            if mid > session_max_id.get(sess, 0):
                session_max_id[sess] = mid
        except Exception:
            pass


async def prune_caches():
    now = time.monotonic()
    async with cache_lock:
        expired = [k for k, (_c, ts) in clients_cache.items() if now - ts > CLIENT_IDLE_SEC]
        for k in expired:
            cli, _ts = clients_cache.pop(k)
            clear_session_state(k)
            await safe_disconnect(cli)
        while len(clients_cache) > MAX_CACHED_CLIENTS:
            oldest_k = min(clients_cache.keys(), key=lambda x: clients_cache[x][1])
            cli, _ts = clients_cache.pop(oldest_k)
            clear_session_state(oldest_k)
            await safe_disconnect(cli)

    async with flow_lock:
        old_f = [k for k, v in pending_flows.items() if now - v.get("ts", 0.0) > FLOW_TTL_SEC]
        for k in old_f:
            fl = pending_flows.pop(k)
            await safe_disconnect(fl.get("client"))


async def get_authed_client(request: web.Request) -> TelegramClient | None:
    sess = get_session_hdr(request)
    if len(sess) < 16 or not api_configured():
        return None
    await prune_caches()
    now = time.monotonic()
    async with cache_lock:
        item = clients_cache.get(sess)
        if item is not None:
            cli, _ = item
            clients_cache[sess] = (cli, now)
        else:
            cli = TelegramClient(StringSession(sess), API_ID, API_HASH)
            clients_cache[sess] = (cli, now)

    try:
        if not cli.is_connected():
            await cli.connect()
        if not await cli.is_user_authorized():
            async with cache_lock:
                clients_cache.pop(sess, None)
            clear_session_state(sess)
            await safe_disconnect(cli)
            return None
        ensure_live_listener(sess, cli)
        return cli
    except Exception:
        async with cache_lock:
            clients_cache.pop(sess, None)
        clear_session_state(sess)
        await safe_disconnect(cli)
        return None


def can_write_entity(ent) -> bool:
    if ent is None:
        return True
    if getattr(ent, "broadcast", False):
        if getattr(ent, "creator", False):
            return True
        ar = getattr(ent, "admin_rights", None)
        return bool(ar and getattr(ar, "post_messages", False))
    if getattr(ent, "creator", False) or getattr(ent, "admin_rights", None):
        return True
    br = getattr(ent, "banned_rights", None)
    if br and getattr(br, "send_messages", False):
        return False
    dbr = getattr(ent, "default_banned_rights", None)
    if dbr and getattr(dbr, "send_messages", False):
        return False
    return True


async def resolve_peer(cli: TelegramClient, chat_ref: str, username: str = "", sess: str = ""):
    ref = str(chat_ref).strip()
    if ref.lower() == "me":
        return "me"
    uname = str(username or "").strip().lstrip("@")
    if sess:
        pc = peer_cache.get(sess)
        if pc:
            if ref in pc:
                return pc[ref]
            uk = (uname or ref.lstrip("@")).lower()
            if uk and uk in pc:
                return pc[uk]
    if ref.lstrip("-").isdigit():
        target_id = int(ref)
        try:
            me = await cli.get_me()
            if me and int(me.id) == target_id:
                return "me"
        except Exception:
            pass
        try:
            pe = await cli.get_input_entity(target_id)
            cache_dialog_peer(sess, ref, uname, pe)
            return pe
        except Exception:
            pass
        if uname:
            try:
                pe = await cli.get_input_entity(uname)
                cache_dialog_peer(sess, ref, uname, pe)
                return pe
            except Exception:
                pass
        dialogs = await cli.get_dialogs(limit=100)
        for d in dialogs:
            d_uname = getattr(d.entity, "username", "") or ""
            cache_dialog_peer(sess, str(d.id), d_uname, d.input_entity)
            try:
                if int(d.id) == target_id or utils.get_peer_id(d.entity) == target_id:
                    return d.input_entity
            except Exception:
                continue
        if ref.startswith("-100") and len(ref) > 4:
            pe = await cli.get_input_entity(PeerChannel(int(ref[4:])))
            cache_dialog_peer(sess, ref, uname, pe)
            return pe
        if target_id < 0:
            pe = await cli.get_input_entity(PeerChat(abs(target_id)))
            cache_dialog_peer(sess, ref, uname, pe)
            return pe
        pe = await cli.get_input_entity(PeerUser(target_id))
        cache_dialog_peer(sess, ref, uname, pe)
        return pe
    u_ref = ref.lstrip("@")
    try:
        pe = await cli.get_input_entity(u_ref if ref.startswith("@") else ref)
        cache_dialog_peer(sess, ref, u_ref, pe)
        return pe
    except Exception:
        return await cli.get_entity(u_ref if ref.startswith("@") else ref)


async def handle_root(_request: web.Request) -> web.Response:
    return json_resp(
        {
            "ok": True,
            "service": "OrbitalOS Telegram Bridge",
            "mode": "cloud-multiuser",
            "api_ready": api_configured(),
        }
    )


async def handle_health(request: web.Request) -> web.Response:
    if not api_configured():
        return json_resp(
            {"ok": False, "authorized": False, "error": "Set TG_API_ID and TG_API_HASH in Render Env"},
            status=503,
        )
    sess = get_session_hdr(request)
    if len(sess) >= 16:
        cli = await get_authed_client(request)
        if cli is not None:
            try:
                me = await cli.get_me()
                return json_resp(
                    {
                        "ok": True,
                        "authorized": True,
                        "user": entity_display_name(me) if me else "",
                        "phone": (getattr(me, "phone", "") or "") if me else "",
                        "need_2fa": False,
                        "hint": "",
                    }
                )
            except Exception as exc:
                return json_resp({"ok": False, "authorized": False, "error": clean_text(str(exc), 80)}, status=500)
        return json_resp({"ok": True, "authorized": False, "user": "", "need_2fa": False, "hint": ""})

    dev_id = get_device_id(request)
    async with flow_lock:
        fl = pending_flows.get(dev_id)
        need_2fa = bool(fl and fl.get("need_2fa", False))
        hint = fl.get("hint", "") if fl else ""
    return json_resp(
        {
            "ok": True,
            "authorized": False,
            "user": "",
            "phone": "",
            "need_2fa": need_2fa,
            "hint": hint,
        }
    )


async def handle_auth_phone(request: web.Request) -> web.Response:
    if not api_configured():
        return json_resp({"ok": False, "error": "Set TG_API_ID and TG_API_HASH in Render Env"}, status=503)
    dev_id = get_device_id(request)
    try:
        body = await request.json()
        phone = str(body.get("phone", "")).strip()
        if len(phone) < 7:
            return json_resp({"ok": False, "error": "Invalid phone number"}, status=400)

        cli = TelegramClient(StringSession(), API_ID, API_HASH)
        await cli.connect()
        sent = await cli.send_code_request(phone)

        async with flow_lock:
            old = pending_flows.pop(dev_id, None)
            if old:
                await safe_disconnect(old.get("client"))
            pending_flows[dev_id] = {
                "client": cli,
                "phone": phone,
                "hash": sent.phone_code_hash,
                "need_2fa": False,
                "hint": "",
                "ts": time.monotonic(),
            }
        print(f"[Auth] Code sent for {dev_id}")
        return json_resp({"ok": True, "status": "code_sent"})
    except PhoneNumberInvalidError:
        return json_resp({"ok": False, "error": "Invalid phone format (+country...)"}, status=400)
    except FloodWaitError as exc:
        return json_resp({"ok": False, "error": f"Wait {exc.seconds}s (FloodWait)"}, status=429)
    except Exception as exc:
        return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=400)


async def finish_login_session(dev_id: str, cli: TelegramClient, user_obj) -> str:
    sess = cli.session.save()
    async with flow_lock:
        pending_flows.pop(dev_id, None)
    async with cache_lock:
        clients_cache[sess] = (cli, time.monotonic())
    ensure_live_listener(sess, cli)
    uname = entity_display_name(user_obj)
    print(f"[Auth] Signed in ({dev_id}) as {uname}")
    return sess


async def handle_auth_code(request: web.Request) -> web.Response:
    if not api_configured():
        return json_resp({"ok": False, "error": "Set TG_API_ID and TG_API_HASH in Render Env"}, status=503)
    dev_id = get_device_id(request)
    try:
        body = await request.json()
        code = "".join(ch for ch in str(body.get("code", "")) if ch.isdigit())
        if len(code) < 4:
            return json_resp({"ok": False, "error": "Enter digits from Telegram code"}, status=400)

        async with flow_lock:
            fl = pending_flows.get(dev_id)
        if not fl or not fl.get("client"):
            return json_resp({"ok": False, "error": "Send phone number first"}, status=400)

        cli: TelegramClient = fl["client"]
        if not cli.is_connected():
            await cli.connect()

        try:
            user = await cli.sign_in(
                phone=fl["phone"],
                code=code,
                phone_code_hash=fl["hash"],
            )
            sess = await finish_login_session(dev_id, cli, user)
            return json_resp({"ok": True, "status": "authorized", "user": entity_display_name(user), "session": sess})
        except SessionPasswordNeededError:
            hint = ""
            try:
                pw_info = await cli(GetPasswordRequest())
                hint = clean_text(getattr(pw_info, "hint", "") or "", 48)
            except Exception:
                hint = ""
            async with flow_lock:
                if dev_id in pending_flows:
                    pending_flows[dev_id]["need_2fa"] = True
                    pending_flows[dev_id]["hint"] = hint
                    pending_flows[dev_id]["ts"] = time.monotonic()
            print(f"[Auth] 2FA required for {dev_id} (hint='{hint}')")
            return json_resp({"ok": True, "status": "2fa_required", "hint": hint})
    except PhoneCodeInvalidError:
        return json_resp({"ok": False, "error": "Wrong code"}, status=400)
    except PhoneCodeExpiredError:
        return json_resp({"ok": False, "error": "Code expired, request again"}, status=400)
    except FloodWaitError as exc:
        return json_resp({"ok": False, "error": f"Wait {exc.seconds}s"}, status=429)
    except Exception as exc:
        return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=400)


async def handle_auth_password(request: web.Request) -> web.Response:
    if not api_configured():
        return json_resp({"ok": False, "error": "Set TG_API_ID and TG_API_HASH in Render Env"}, status=503)
    dev_id = get_device_id(request)
    try:
        body = await request.json()
        pwd = str(body.get("password", ""))
        if not pwd:
            return json_resp({"ok": False, "error": "Password is empty"}, status=400)

        async with flow_lock:
            fl = pending_flows.get(dev_id)
        if not fl or not fl.get("client"):
            return json_resp({"ok": False, "error": "Session not found, send phone first"}, status=400)

        cli: TelegramClient = fl["client"]
        if not cli.is_connected():
            await cli.connect()

        user = await cli.sign_in(password=pwd)
        sess = await finish_login_session(dev_id, cli, user)
        return json_resp({"ok": True, "status": "authorized", "user": entity_display_name(user), "session": sess})
    except PasswordHashInvalidError:
        return json_resp({"ok": False, "error": "Wrong cloud password"}, status=400)
    except FloodWaitError as exc:
        return json_resp({"ok": False, "error": f"Wait {exc.seconds}s"}, status=429)
    except Exception as exc:
        return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=400)


async def handle_auth_logout(request: web.Request) -> web.Response:
    dev_id = get_device_id(request)
    sess = get_session_hdr(request)
    async with flow_lock:
        old = pending_flows.pop(dev_id, None)
        if old:
            await safe_disconnect(old.get("client"))
    if len(sess) >= 16:
        cli = await get_authed_client(request)
        if cli is not None:
            try:
                await cli.log_out()
            except Exception:
                pass
            await safe_disconnect(cli)
        async with cache_lock:
            clients_cache.pop(sess, None)
        clear_session_state(sess)
    return json_resp({"ok": True, "status": "logged_out"})


def msg_file_size_bytes(msg) -> int:
    fobj = getattr(msg, "file", None)
    sz = int(getattr(fobj, "size", 0) or 0) if fobj is not None else 0
    if sz <= 0:
        doc = getattr(msg, "document", None) or getattr(msg, "video", None)
        if doc is not None:
            sz = int(getattr(doc, "size", 0) or 0)
    return sz


def msg_summary_ext(msg) -> tuple[str, bool, bool]:
    has_photo = bool(getattr(msg, "photo", None))
    fsize = msg_file_size_bytes(msg)
    txt = getattr(msg, "message", None) or getattr(msg, "text", None) or ""
    txt = clean_text(txt, 200)
    if has_photo:
        if fsize > MAX_MEDIA_BYTES:
            return (("[Photo >4MB - too large] " + txt).strip()), False, True
        return txt, True, False
    if fsize > MAX_MEDIA_BYTES:
        mb = max(1, fsize // (1024 * 1024))
        tag = f"[File {mb}MB > limit]"
        return (f"{tag} {txt}".strip() if txt else tag), False, True
    if not txt:
        if getattr(msg, "voice", None):
            txt = "[Voice]"
        elif getattr(msg, "sticker", None):
            txt = "[Sticker]"
        elif getattr(msg, "video", None):
            txt = "[Video]"
        elif getattr(msg, "document", None):
            txt = "[File]"
        else:
            txt = "[Media]"
    return txt, False, False


async def handle_chats(request: web.Request) -> web.Response:
    cli = await get_authed_client(request)
    if cli is None:
        return json_resp({"ok": False, "error": "unauthorized"}, status=401)
    sess = get_session_hdr(request)
    async with get_session_lock(sess):
        try:
            limit = max(1, min(12, int(request.query.get("limit", "8"))))
            dialogs = await cli.get_dialogs(limit=limit)
            last_dialog_sync[sess] = time.monotonic()
            chats = []
            max_id = session_max_id.get(sess, 0)
            for d in dialogs:
                uname = getattr(d.entity, "username", "") or ""
                cache_dialog_peer(sess, str(d.id), uname, d.input_entity)
                last_txt = ""
                if d.message:
                    mid = int(d.message.id)
                    if mid > max_id:
                        max_id = mid
                    t, is_ph, _tb = msg_summary_ext(d.message)
                    last_txt = ("[Photo] " + t).strip() if is_ph else t
                    last_txt = clean_text(last_txt, 60)
                chats.append(
                    {
                        "id": str(d.id),
                        "name": entity_display_name(d.entity),
                        "username": uname,
                        "unread": bool(d.unread_count > 0),
                        "pinned": bool(d.pinned),
                        "can_write": can_write_entity(d.entity),
                        "last": last_txt,
                    }
                )
            if max_id > session_max_id.get(sess, 0):
                session_max_id[sess] = max_id
            return json_resp({"ok": True, "chats": chats})
        except Exception as exc:
            return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=500)


async def handle_messages(request: web.Request) -> web.Response:
    cli = await get_authed_client(request)
    if cli is None:
        return json_resp({"ok": False, "error": "unauthorized"}, status=401)
    sess = get_session_hdr(request)
    async with get_session_lock(sess):
        try:
            chat_id = request.query.get("chat_id", "").strip()
            username = request.query.get("username", "").strip()
            if not chat_id:
                return json_resp({"ok": False, "error": "chat_id required"}, status=400)
            limit = max(1, min(12, int(request.query.get("limit", "10"))))
            peer = await resolve_peer(cli, chat_id, username, sess=sess)
            writable = True
            try:
                ent = await cli.get_entity(peer)
                writable = can_write_entity(ent)
            except Exception:
                pass
            msgs = await cli.get_messages(peer, limit=limit)
            out = []
            max_id = session_max_id.get(sess, 0)
            for m in reversed(msgs):
                if m is None:
                    continue
                mid = int(m.id)
                if mid > max_id:
                    max_id = mid
                txt, has_photo, too_big = msg_summary_ext(m)
                out.append(
                    {
                        "id": mid,
                        "me": bool(m.out),
                        "text": txt,
                        "photo": bool(has_photo),
                        "too_big": bool(too_big),
                    }
                )
            if max_id > session_max_id.get(sess, 0):
                session_max_id[sess] = max_id
            return json_resp({"ok": True, "can_write": writable, "msgs": out})
        except Exception as exc:
            return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=500)


async def handle_updates(request: web.Request) -> web.Response:
    cli = await get_authed_client(request)
    if cli is None:
        return json_resp({"ok": False, "error": "unauthorized"}, status=401)
    sess = get_session_hdr(request)
    async with get_session_lock(sess):
        try:
            since = int(request.query.get("since", "0"))
            q = inbox_queues.setdefault(sess, deque(maxlen=24))
            has_queued_new = any(it["id"] > since for it in q) if since > 0 else False
            need_sync = (since <= 0) or (sess not in session_max_id) or (
                not has_queued_new and (time.monotonic() - last_dialog_sync.get(sess, 0.0) >= 20.0)
            )
            max_id = max(since, session_max_id.get(sess, 0))
            if need_sync:
                dialogs = await cli.get_dialogs(limit=8)
                last_dialog_sync[sess] = time.monotonic()
                for d in dialogs:
                    uname = getattr(d.entity, "username", "") or ""
                    cache_dialog_peer(sess, str(d.id), uname, d.input_entity)
                    m = d.message
                    if not m:
                        continue
                    mid = int(m.id)
                    if mid > max_id:
                        max_id = mid
                    if since > 0 and mid > since and not m.out:
                        cid = str(d.id)
                        if not any(x["id"] == mid and x["chat_id"] == cid for x in q):
                            txt, has_photo, too_big = msg_summary_ext(m)
                            q.append(
                                {
                                    "id": mid,
                                    "chat_id": cid,
                                    "sender": entity_display_name(d.entity),
                                    "username": uname,
                                    "text": txt,
                                    "photo": bool(has_photo),
                                    "too_big": bool(too_big),
                                }
                            )
            for it in q:
                if it["id"] > max_id:
                    max_id = it["id"]
            if max_id > session_max_id.get(sess, 0):
                session_max_id[sess] = max_id
            items = [it for it in q if since > 0 and it["id"] > since]
            items.sort(key=lambda x: x["id"])
            return json_resp({"ok": True, "max_id": max_id, "items": items[:6]})
        except Exception as exc:
            return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=500)


async def _do_send_text(cli: TelegramClient, chat_id: str, username: str, text: str, sess: str = "") -> web.Response:
    if not chat_id or not text:
        return json_resp({"ok": False, "error": "chat_id/text required"}, status=400)
    peer = await resolve_peer(cli, chat_id, username, sess=sess)
    msg = await cli.send_message(peer, text)
    if sess and int(msg.id) > session_max_id.get(sess, 0):
        session_max_id[sess] = int(msg.id)
    print(f"[Send] OK to {chat_id} msg_id={msg.id}", flush=True)
    return json_resp({"ok": True, "id": int(msg.id)})


async def handle_send(request: web.Request) -> web.Response:
    cli = await get_authed_client(request)
    if cli is None:
        return json_resp({"ok": False, "error": "unauthorized"}, status=401)
    sess = get_session_hdr(request)
    async with get_session_lock(sess):
        try:
            raw = await request.read()
            body = json.loads(raw.decode("utf-8", errors="replace"))
            chat_id = str(body.get("chat_id", "")).strip()
            username = str(body.get("username", "")).strip()
            text = str(body.get("text", ""))
            return await _do_send_text(cli, chat_id, username, text, sess=sess)
        except Exception as exc:
            print(f"[Send] ERR POST: {exc}", flush=True)
            return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=400)


async def handle_send_get(request: web.Request) -> web.Response:
    cli = await get_authed_client(request)
    if cli is None:
        return json_resp({"ok": False, "error": "unauthorized"}, status=401)
    sess = get_session_hdr(request)
    async with get_session_lock(sess):
        try:
            chat_id = request.query.get("chat_id", "").strip()
            username = request.query.get("username", "").strip()
            text = request.query.get("text", "")
            return await _do_send_text(cli, chat_id, username, text, sess=sess)
        except Exception as exc:
            print(f"[Send] ERR GET: {exc}", flush=True)
            return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=400)


async def handle_send_photo(request: web.Request) -> web.Response:
    cli = await get_authed_client(request)
    if cli is None:
        return json_resp({"ok": False, "error": "unauthorized"}, status=401)
    sess = get_session_hdr(request)
    async with get_session_lock(sess):
        try:
            chat_id = request.query.get("chat_id", "").strip()
            username = request.query.get("username", "").strip()
            if not chat_id:
                return json_resp({"ok": False, "error": "chat_id required"}, status=400)
            raw = await request.read()
            if len(raw) < 32:
                return json_resp({"ok": False, "error": "empty image"}, status=400)
            if len(raw) > MAX_UPLOAD_PHOTO_BYTES:
                return json_resp({"ok": False, "error": "File too large (>1 MB)"}, status=413)
            bio = io.BytesIO(raw)
            bio.name = "lymo_photo.png"
            peer = await resolve_peer(cli, chat_id, username, sess=sess)
            msg = await cli.send_file(peer, bio, force_document=False)
            return json_resp({"ok": True, "id": int(msg.id)})
        except Exception as exc:
            return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=400)


async def handle_photo(request: web.Request) -> web.Response:
    cli = await get_authed_client(request)
    if cli is None:
        return web.Response(status=401, text="unauthorized")
    sess = get_session_hdr(request)
    async with get_session_lock(sess):
        try:
            chat_id = request.query.get("chat_id", "").strip()
            msg_id = int(request.query.get("msg_id", "0"))
            if not chat_id or msg_id <= 0:
                return web.Response(status=400, text="bad params")
            peer = await resolve_peer(cli, chat_id, sess=sess)
            msg = await cli.get_messages(peer, ids=msg_id)
            if not msg:
                return web.Response(status=404, text="not found")
            if msg_file_size_bytes(msg) > MAX_MEDIA_BYTES:
                return web.Response(status=413, text="File too large (>4 MB)")
            raw = await cli.download_media(msg, file=bytes)
            if not raw:
                return web.Response(status=404, text="no media")

            tw, th = 128, 96
            resamp = getattr(Image, "Resampling", Image).LANCZOS
            img = Image.open(io.BytesIO(raw)).convert("RGB")
            img.thumbnail((tw, th), resamp)
            canvas = Image.new("RGB", (tw, th), (24, 37, 51))
            ox = (tw - img.width) // 2
            oy = (th - img.height) // 2
            canvas.paste(img, (ox, oy))

            out = io.BytesIO()
            canvas.save(out, format="JPEG", quality=76, progressive=False, optimize=False)
            return web.Response(body=out.getvalue(), content_type="image/jpeg")
        except Exception as exc:
            return web.Response(status=500, text=clean_text(str(exc), 80))


async def handle_resolve(request: web.Request) -> web.Response:
    cli = await get_authed_client(request)
    if cli is None:
        return json_resp({"ok": False, "error": "unauthorized"}, status=401)
    sess = get_session_hdr(request)
    async with get_session_lock(sess):
        try:
            q = request.query.get("q", "").strip().lstrip("@")
            if len(q) < 3:
                return json_resp({"ok": False, "error": "short query"}, status=400)
            ent = await cli.get_entity(q)
            peer_id = str(getattr(ent, "id", ""))
            if ent.__class__.__name__ == "Channel" and not peer_id.startswith("-"):
                peer_id = "-100" + peer_id
            elif ent.__class__.__name__ == "Chat" and not peer_id.startswith("-"):
                peer_id = "-" + peer_id
            uname = getattr(ent, "username", "") or ""
            try:
                cache_dialog_peer(sess, peer_id, uname, utils.get_input_peer(ent))
            except Exception:
                pass
            return json_resp(
                {
                    "ok": True,
                    "id": peer_id,
                    "name": entity_display_name(ent),
                    "username": uname,
                    "can_write": can_write_entity(ent),
                }
            )
        except Exception as exc:
            return json_resp({"ok": False, "error": clean_text(str(exc), 80)}, status=404)


async def run_server():
    global API_ID, API_HASH
    API_ID, API_HASH, port = load_config()

    app = web.Application(client_max_size=2 * 1024 * 1024)
    app.router.add_get("/", handle_root)
    app.router.add_get("/health", handle_health)
    app.router.add_post("/auth/phone", handle_auth_phone)
    app.router.add_post("/auth/code", handle_auth_code)
    app.router.add_post("/auth/password", handle_auth_password)
    app.router.add_post("/auth/logout", handle_auth_logout)
    app.router.add_get("/chats", handle_chats)
    app.router.add_get("/messages", handle_messages)
    app.router.add_get("/updates", handle_updates)
    app.router.add_get("/send_msg", handle_send_get)
    app.router.add_post("/send", handle_send)
    app.router.add_post("/send_photo", handle_send_photo)
    app.router.add_get("/photo", handle_photo)
    app.router.add_get("/resolve", handle_resolve)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    ip = get_lan_ip()
    print("=" * 56)
    print(f"  OrbitalOS Multi-User Telegram Bridge running on port {port}")
    print(f"  Local URL: http://{ip}:{port}")
    print(f"  API Env Configured: {api_configured()}")
    print("=" * 56)

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(run_server())
    except KeyboardInterrupt:
        print("\nBridge stopped.")
        sys.exit(0)
