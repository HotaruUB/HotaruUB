from __future__ import annotations

import asyncio
import random
import re
import secrets
import string
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

BOTFATHER = "@BotFather"
BOTFATHER_ID = 93372553
TOKEN_RE = re.compile(r"\d{6,}:[A-Za-z0-9_-]{35}")
USERNAME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{2,31}bot$", re.IGNORECASE)


class InlineError(RuntimeError):
    pass


@dataclass(slots=True)
class InlineBotInfo:
    token: str
    username: str
    bot_id: int


class BotFatherConversation:
    def __init__(self, app: Any, *, timeout: float = 30.0) -> None:
        self.app = app
        self.timeout = timeout
        self._peer: Any = None
        self._last_id = 0
        self._self_id = getattr(getattr(app, "mt", None), "self_id", None) or getattr(app, "self_id", None) or 0

    async def __aenter__(self) -> "BotFatherConversation":
        self._peer = await self.app.mt.resolve_peer(BOTFATHER)
        state = await self.app.mt_req(
            "messages.getHistory",
            peer=self._peer,
            offset_id=0,
            offset_date=0,
            add_offset=0,
            limit=1,
            max_id=0,
            min_id=0,
            hash=0,
        )
        body = state.get("result") if isinstance(state, dict) and isinstance(state.get("result"), dict) else state
        messages = body.get("messages") if isinstance(body, dict) else None
        if messages:
            self._last_id = max((m.get("id", 0) for m in messages if isinstance(m, dict)), default=0)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def say(self, text: str) -> int:
        result = await self.app.mt_req(
            "messages.sendMessage",
            peer=self._peer,
            message=text,
            random_id=secrets.randbits(63),
        )
        return self._extract_sent_id(result)

    def _extract_sent_id(self, result: Any) -> int:
        body = result.get("result") if isinstance(result, dict) and isinstance(result.get("result"), dict) else result
        if isinstance(body, dict):
            updates = body.get("updates")
            if isinstance(updates, list):
                for update in updates:
                    if not isinstance(update, dict):
                        continue
                    kind = update.get("_")
                    if kind in ("updateNewMessage", "updateMessageID"):
                        message = update.get("message")
                        if isinstance(message, dict) and isinstance(message.get("id"), int):
                            return message["id"]
                    if kind == "updateMessageID" and isinstance(update.get("id"), int):
                        return update["id"]
        return 0

    async def _delete(self, *ids: int) -> None:
        valid = [i for i in ids if isinstance(i, int) and i > 0]
        if not valid:
            return
        try:
            await self.app.mt_req("messages.deleteMessages", id=valid, revoke=True)
        except Exception:
            pass

    def _is_mine(self, message: dict[str, Any]) -> bool:
        if message.get("out"):
            return True
        sender = message.get("from_id")
        if isinstance(sender, int):
            return sender == self._self_id
        if isinstance(sender, dict):
            user_id = sender.get("user_id")
            return isinstance(user_id, int) and user_id == self._self_id
        return False

    async def response(self, *, since: int | None = None) -> dict[str, Any]:
        floor = since if since is not None else self._last_id
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            result = await self.app.mt_req(
                "messages.getHistory",
                peer=self._peer,
                offset_id=0,
                offset_date=0,
                add_offset=0,
                limit=10,
                max_id=0,
                min_id=0,
                hash=0,
            )
            body = result.get("result") if isinstance(result, dict) and isinstance(result.get("result"), dict) else result
            messages = body.get("messages") if isinstance(body, dict) else None
            for message in messages or []:
                if not isinstance(message, dict):
                    continue
                if message.get("id", 0) <= floor:
                    continue
                if self._is_mine(message):
                    self._last_id = max(self._last_id, message["id"])
                    continue
                self._last_id = max(self._last_id, message["id"])
                return message
            await asyncio.sleep(1.0)
        raise InlineError("BotFather response timeout")

    async def drain(self) -> None:
        for _ in range(4):
            result = await self.app.mt_req(
                "messages.getHistory",
                peer=self._peer,
                offset_id=0,
                offset_date=0,
                add_offset=0,
                limit=10,
                max_id=0,
                min_id=0,
                hash=0,
            )
            body = result.get("result") if isinstance(result, dict) and isinstance(result.get("result"), dict) else result
            messages = body.get("messages") if isinstance(body, dict) else None
            recent = [m for m in messages or [] if isinstance(m, dict) and m.get("id", 0) > self._last_id]
            if not recent:
                return
            for message in recent:
                self._last_id = max(self._last_id, message.get("id", 0))
            await self._delete(*(m.get("id", 0) for m in recent))
            await asyncio.sleep(0.5)

    async def ask(self, text: str) -> dict[str, Any]:
        await self.drain()
        mine_id = await self.say(text)
        try:
            reply = await self.response()
        except InlineError:
            await self._delete(mine_id)
            raise
        await self._delete(mine_id, reply.get("id", 0))
        return reply

class BotFatherGuard:
    def __init__(self, app: Any) -> None:
        self.app = app
        self._peer: bytes | None = None
        self._was_archived: bool | None = None
        self._was_muted: bool | None = None

    async def _resolve(self) -> bytes:
        if self._peer is None:
            self._peer = await self.app.mt.resolve_peer(BOTFATHER)
        return self._peer

    async def _dialog(self) -> dict[str, Any] | None:
        peer = await self._resolve()
        result = await self.app.mt_req(
            "messages.getPeerDialogs",
            peers=[{"_": "inputDialogPeer", "peer": peer}],
        )
        body = result.get("result") if isinstance(result, dict) and isinstance(result.get("result"), dict) else result
        for dialog in (body.get("dialogs") or []) if isinstance(body, dict) else []:
            if isinstance(dialog, dict):
                return dialog
        return None

    async def read_state(self) -> dict[str, Any]:
        dialog = await self._dialog()
        if dialog is None:
            return {"archived": False, "muted": False}
        settings = dialog.get("notify_settings") if isinstance(dialog.get("notify_settings"), dict) else {}
        mute_until = settings.get("mute_until") or 0
        return {
            "archived": isinstance(dialog.get("folder_id"), int) and dialog["folder_id"] != 0,
            "muted": isinstance(mute_until, int) and mute_until > int(time.time()),
        }

    async def _folder_peer(self, folder_id: int) -> dict[str, Any]:
        await self._resolve()
        entity = self.app.mt.entity_usernames.get("botfather") or self.app.mt.entities.get(("user", BOTFATHER_ID)) or {}
        access_hash = entity.get("access_hash") if isinstance(entity, dict) else 0
        return {
            "_": "inputFolderPeer",
            "peer": {"_": "inputPeerUser", "user_id": BOTFATHER_ID, "access_hash": int(access_hash or 0)},
            "folder_id": folder_id,
        }

    async def set_archived(self, archived: bool) -> None:
        await self.app.mt_req(
            "folders.editPeerFolders",
            folder_peers=[await self._folder_peer(1 if archived else 0)],
        )

    async def set_muted(self, muted: bool) -> None:
        peer = await self._resolve()
        await self.app.mt_req(
            "account.updateNotifySettings",
            peer={"_": "inputNotifyPeer", "peer": peer},
            settings={"_": "inputPeerNotifySettings", "mute_until": 2147483647 if muted else 0},
        )

    async def __aenter__(self) -> "BotFatherGuard":
        try:
            state = await self.read_state()
            self._was_archived = state["archived"]
            self._was_muted = state["muted"]
            if not state["archived"]:
                await self.set_archived(True)
            if not state["muted"]:
                await self.set_muted(True)
        except Exception:
            self._was_archived = None
            self._was_muted = None
        return self

    async def __aexit__(self, *exc: Any) -> None:
        try:
            if self._was_archived is False:
                await self.set_archived(False)
            if self._was_muted is False:
                await self.set_muted(False)
        except Exception:
            pass


class InlineManager:
    def __init__(
        self,
        runtime: Any,
        *,
        bot_name: str = "Hotaru userbot",
        inline_placeholder: str = "hotaru:~$",
        poll_timeout: int = 25,
    ) -> None:
        self.runtime = runtime
        self.bot_name = bot_name
        self.inline_placeholder = inline_placeholder
        self.poll_timeout = poll_timeout
        self.bot_app: Any = None
        self.info: InlineBotInfo | None = None
        self._task: asyncio.Task[None] | None = None
        self._handlers: list[Callable[[Any], Awaitable[Any]]] = []
        self._cb_handlers: list[Callable[[Any], Awaitable[Any]]] = []
        self._pm_handlers: list[Callable[[Any], Awaitable[Any]]] = []
        self._stop = asyncio.Event()
        self.ready = asyncio.Event()
        self._create_attempts: list[float] = []
        self._provision_lock = asyncio.Lock()

    def on_inline(self, handler: Callable[[Any], Awaitable[Any]]) -> Callable[[Any], Awaitable[Any]]:
        self._handlers.append(handler)
        return handler

    def on_callback(self, handler: Callable[[Any], Awaitable[Any]]) -> Callable[[Any], Awaitable[Any]]:
        self._cb_handlers.append(handler)
        return handler

    def on_bot_pm(self, handler: Callable[[Any], Awaitable[Any]]) -> Callable[[Any], Awaitable[Any]]:
        self._pm_handlers.append(handler)
        return handler

    async def ensure_bot(self, *, allow_create: bool = True) -> InlineBotInfo:
        state = self.runtime.state
        if state is None:
            raise InlineError("state store is not ready")
        token = state.get_setting("inline-bot-token")
        bot_id = state.get_setting("inline-bot-id")
        if not token and self.runtime.config.api_id is not None:
            token = self.runtime.config.bot_token
            bot_id = None
        if token:
            info = await self.getbot(str(token))
            if bot_id is not None and info.bot_id != int(bot_id):
                raise InlineError("inline bot identity does not match the stored configuration")
            state.set_setting("inline-bot-token", info.token)
            state.set_setting("inline-bot-username", info.username)
            state.set_setting("inline-bot-id", info.bot_id)
            self.info = info
            return info
        info = await self._find_existing_bot()
        if info is None and allow_create:
            self._create_gate()
            async with self._provision_lock:
                info = await self._create_bot()
        if info is None:
            raise InlineError("no inline bot available: nothing stored, nothing found, creation disabled")
        state.set_setting("inline-bot-token", info.token)
        state.set_setting("inline-bot-username", info.username)
        state.set_setting("inline-bot-id", info.bot_id)
        self.info = info
        return info

    def _create_gate(self, *, max_attempts: int = 3, window: float = 3600.0, cooldown: float = 900.0) -> None:
        now = time.monotonic()
        self._create_attempts = [t for t in self._create_attempts if now - t < window]
        if len(self._create_attempts) >= max_attempts:
            last = self._create_attempts[-1]
            waited = now - last
            if waited < cooldown:
                raise InlineError(
                    f"provisioning cooldown: {max_attempts} creations in the last hour, retry in {int(cooldown - waited)}s"
                )
        self._create_attempts.append(now)

    async def getbot(self, token: str) -> InlineBotInfo:
        from goygram import GoyGram

        app = GoyGram(bot_token=token, bot_timeout=10, default_transport="api")
        try:
            payload = await app.bot_req("getMe")
            result = payload if isinstance(payload, dict) else None
            if not isinstance(result, dict):
                raise InlineError("inline bot token validation failed")
            botid = result.get("id")
            username = result.get("username")
            if not isinstance(botid, int) or botid <= 0 or not isinstance(username, str) or not username or result.get("is_bot") is not True:
                raise InlineError("inline bot identity is incomplete")
            return InlineBotInfo(token, username.lstrip("@"), botid)
        except Exception:
            raise InlineError("inline bot token validation failed") from None
        finally:
            await app.close()

    def _forget_bot(self) -> None:
        state = self.runtime.state
        if state is None:
            return
        for key in ("inline-bot-token", "inline-bot-username", "inline-bot-id"):
            state.set_setting(key, None)
        self.info = None

    async def _find_existing_bot(self) -> InlineBotInfo | None:
        state = self.runtime.state
        wanted = state.get_setting("inline-bot-username") if state else None
        candidates: list[tuple[str, str]] = []
        try:
            async with BotFatherGuard(self.runtime.app), BotFatherConversation(self.runtime.app) as conv:
                response = await conv.ask("/mybots")
                markup = response.get("reply_markup")
                rows = markup.get("rows") if isinstance(markup, dict) else None
                if not rows:
                    return None
                for row in rows:
                    for button in row.get("buttons", []):
                        text = button.get("text", "")
                        candidate = text.lstrip("@")
                        if not USERNAME_RE.match(candidate):
                            continue
                        if wanted and candidate.casefold() == str(wanted).casefold():
                            candidates.insert(0, (text, candidate))
                        elif candidate.lower().startswith("hotaru") or not wanted:
                            candidates.append((text, candidate))
                for button_text, candidate in candidates:
                    token = await self._fetch_token(conv, button_text)
                    if token is None:
                        continue
                    bot_id = int(token.split(":", 1)[0])
                    return InlineBotInfo(token, candidate, bot_id)
        except InlineError:
            return None
        except Exception:
            return None
        return None

    async def _fetch_token(self, conv: BotFatherConversation, username_button: str) -> str | None:
        try:
            await conv.ask("/token")
            answer = await conv.ask(username_button)
            text = answer.get("message", "")
            match = TOKEN_RE.search(text)
            return match.group(0) if match else None
        except Exception:
            return None

    async def _create_bot(self) -> InlineBotInfo:
        async with BotFatherGuard(self.runtime.app), BotFatherConversation(self.runtime.app) as conv:
            response = await conv.ask("/newbot")
            text = response.get("message", "")
            lowered = text.lower()
            if "cannot create new bots" in lowered or "contact @spambot" in lowered or "cannot create" in lowered:
                raise InlineError("BotFather spamban: account cannot create new bots, contact @SpamBot")
            if "too many" in lowered or "up to 20" in lowered or "limit" in lowered:
                raise InlineError("BotFather limit reached: max 20 bots per account")
            if "a new bot" not in lowered:
                raise InlineError("BotFather refused: " + text.splitlines()[0][:120] if text else "BotFather refused")
            await conv.ask(self.bot_name[:64])
            username, token = await self._pick_username(conv)
            bot_id = int(token.split(":", 1)[0])
            await self._configure(conv, username)
            return InlineBotInfo(token, username, bot_id)

    async def _pick_username(self, conv: BotFatherConversation) -> tuple[str, str]:
        for _ in range(8):
            suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
            username = f"hotaru_{suffix}_bot"
            response = await conv.ask(username)
            text = response.get("message", "")
            lowered = text.lower()
            if "sorry" in lowered or "taken" in lowered or "invalid" in lowered or "occupied" in lowered:
                continue
            match = TOKEN_RE.search(text)
            if match is None:
                continue
            return username, match.group(0)
        raise InlineError("could not allocate a bot username")

    async def _configure(self, conv: BotFatherConversation, username: str) -> None:
        at = f"@{username}"
        for step in (
            ("/setinline", at, self.inline_placeholder),
            ("/setinlinefeedback", at, "Enabled"),
        ):
            retried = False
            for message in step:
                try:
                    response = await conv.ask(message)
                except InlineError:
                    if retried:
                        return
                    retried = True
                    if self.runtime.observatory is not None:
                        self.runtime.observatory.emit("inline", "configure_retry", step=step[0], message=message)
                    continue
                text = response.get("message", "").lower()
                if "invalid bot selected" in text and not retried:
                    retried = True
                    await conv.ask(message)
                if "invalid bot selected" in text and retried:
                    if self.runtime.observatory is not None:
                        self.runtime.observatory.emit("inline", "configure_desync", step=step[0])
                    return

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        if self.info is None:
            await self.ensure_bot()
        assert self.info is not None
        from goygram import GoyGram

        main = self.runtime.app
        if main is not None and main.core.bot_token == self.info.token:
            raise InlineError("inline polling requires a token separate from the primary bot")
        self._stop.clear()
        self.ready.clear()
        self.bot_app = GoyGram(
            bot_token=self.info.token,
            bot_timeout=self.poll_timeout,
            default_transport="api",
        )
        self.bot_app.on_inline(self._dispatch_inline)
        self.bot_app.on_cb(self._dispatch_callback)
        self.bot_app.on_msg(self._dispatch_bot_pm)
        self._task = asyncio.create_task(self._run(), name="hotaru:inline-bot")

    async def _run(self) -> None:
        app = self.bot_app
        assert app is not None
        delay = 1.0
        while not self._stop.is_set():
            dispatch_task = None
            try:
                await app.core.bot.boot()
                await app.bot_req("deleteWebhook", drop_pending_updates=False)
                dispatch_task = asyncio.create_task(app.core.disp.consume(), name="hotaru:inline-dispatch")
                self.ready.set()
                await app.core.bot.spin()
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if self._is_auth_failure(exc):
                    if self.runtime.observatory is not None:
                        self.runtime.observatory.emit("inline", "token_revoked_midflight")
                    self._forget_bot()
                    await self.stop_polling()
                    try:
                        await self.ensure_bot()
                        await self.start()
                    except Exception as retry_exc:
                        if self.runtime.observatory is not None:
                            self.runtime.observatory.emit("inline", "reprovision_failed", error=type(retry_exc).__name__)
                    return
                if self.runtime.observatory is not None:
                    self.runtime.observatory.emit("inline", "poll_error", error=type(exc).__name__)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
                delay = min(delay * 2, 60.0)
            finally:
                if app is self.bot_app:
                    self.ready.clear()
                if dispatch_task is not None and not dispatch_task.done():
                    dispatch_task.cancel()
                    await asyncio.gather(dispatch_task, return_exceptions=True)

    @staticmethod
    def _is_auth_failure(exc: Exception) -> bool:
        text = str(exc).lower()
        if "http 401" in text or "unauthorized" in text:
            return True
        return "token" in text and ("invalid" in text or "revoked" in text)

    async def stop_polling(self) -> None:
        self.ready.clear()
        task = self._task
        self._task = None
        if task is not None and task is not asyncio.current_task() and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if self.bot_app is not None:
            try:
                self.bot_app.stop()
                await self.bot_app.close()
            except Exception:
                pass

    async def _dispatch_inline(self, query: Any) -> None:
        for handler in tuple(self._handlers):
            try:
                await handler(query)
            except Exception as exc:
                if self.runtime.observatory is not None:
                    self.runtime.observatory.emit("inline", "handler_error", error=type(exc).__name__, detail=str(exc)[:240])

    @staticmethod
    def _form_article(text: str, buttons: list[dict[str, str]]) -> dict[str, Any]:
        from goygram.types import InlineObj

        result = InlineObj.article("hotaru-form", "Hotaru form", text)
        result["reply_markup"] = {"inline_keyboard": [buttons]}
        return result

    async def _dispatch_callback(self, callback: Any) -> None:
        security = getattr(self.runtime, "security", None)
        if security is not None:
            from hotaru.security import AccessVerdict

            verdict = security.check_callback(callback, transport="inline")
            if verdict is not AccessVerdict.ALLOW:
                return
        for handler in tuple(self._cb_handlers):
            try:
                await handler(callback)
            except Exception as exc:
                if self.runtime.observatory is not None:
                    self.runtime.observatory.emit("inline", "callback_error", error=type(exc).__name__, detail=str(exc)[:240])

    async def _dispatch_bot_pm(self, message: Any) -> None:
        security = getattr(self.runtime, "security", None)
        if security is not None:
            from hotaru.security import AccessVerdict

            verdict = security.check(message, transport="bot-pm")
            if verdict is not AccessVerdict.ALLOW:
                return
        for handler in tuple(self._pm_handlers):
            try:
                await handler(message)
            except Exception as exc:
                if self.runtime.observatory is not None:
                    self.runtime.observatory.emit("inline", "pm_handler_error", error=type(exc).__name__)

    async def stop(self) -> None:
        self._stop.set()
        await self.stop_polling()
