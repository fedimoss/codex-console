"""
Temp-Mail 邮箱服务实现
基于自部署 Cloudflare Worker 临时邮箱服务
接口文档参见 plan/temp-mail.md
"""

import re
import time
import json
import logging
from datetime import datetime, timezone
from email import message_from_string
from email.header import decode_header, make_header
from email.message import Message
from email.policy import default as email_policy
from html import unescape
from typing import Optional, Dict, Any, List

from .base import BaseEmailService, EmailServiceError, EmailServiceType
from ..core.http_client import HTTPClient, RequestConfig
from ..config.constants import OTP_CODE_PATTERN


logger = logging.getLogger(__name__)


class TempMailService(BaseEmailService):
    """
    Temp-Mail 邮箱服务
    基于自部署 Cloudflare Worker 的临时邮箱，admin 模式管理邮箱
    不走代理，不使用 requests 库
    """

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        """
        初始化 TempMail 服务

        Args:
            config: 配置字典，支持以下键:
                - base_url: Worker 域名地址，如 https://mail.example.com (必需)
                - admin_password: Admin 密码，对应 x-admin-auth header (必需)
                - domain: 邮箱域名，如 example.com (必需)
                - enable_prefix: 是否启用前缀，默认 True
                - timeout: 请求超时时间，默认 30
                - max_retries: 最大重试次数，默认 3
            name: 服务名称
        """
        super().__init__(EmailServiceType.TEMP_MAIL, name)

        required_keys = ["base_url", "admin_password", "domain"]
        missing_keys = [key for key in required_keys if not (config or {}).get(key)]
        if missing_keys:
            raise ValueError(f"缺少必需配置: {missing_keys}")

        default_config = {
            "enable_prefix": True,
            "timeout": 30,
            "max_retries": 3,
        }
        self.config = {**default_config, **(config or {})}

        # 不走代理，proxy_url=None
        http_config = RequestConfig(
            timeout=self.config["timeout"],
            max_retries=self.config["max_retries"],
        )
        self.http_client = HTTPClient(proxy_url=None, config=http_config)

        # 邮箱缓存：email -> {jwt, address}
        self._email_cache: Dict[str, Dict[str, Any]] = {}

        # 已使用验证码缓存：避免同邮箱重复返回旧验证码（兜底）
        # 说明：这是进程内缓存，重启会丢失；建议与 otp_sent_at 过滤配合使用
        self._used_codes: Dict[str, set] = {}

    def _decode_mime_header(self, value: str) -> str:
        """解码 MIME 头，兼容 RFC 2047 编码主题。"""
        if not value:
            return ""
        try:
            return str(make_header(decode_header(value)))
        except Exception:
            return value

    def _extract_body_from_message(self, message: Message) -> str:
        """从 MIME 邮件对象中提取可读正文。"""
        parts: List[str] = []

        if message.is_multipart():
            for part in message.walk():
                if part.get_content_maintype() == "multipart":
                    continue

                content_type = (part.get_content_type() or "").lower()
                if content_type not in ("text/plain", "text/html"):
                    continue

                try:
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    text = payload.decode(charset, errors="replace") if payload else ""
                except Exception:
                    try:
                        text = part.get_content()
                    except Exception:
                        text = ""

                if content_type == "text/html":
                    text = re.sub(r"<[^>]+>", " ", text)
                parts.append(text)
        else:
            try:
                payload = message.get_payload(decode=True)
                charset = message.get_content_charset() or "utf-8"
                body = payload.decode(charset, errors="replace") if payload else ""
            except Exception:
                try:
                    body = message.get_content()
                except Exception:
                    body = str(message.get_payload() or "")

            if "html" in (message.get_content_type() or "").lower():
                body = re.sub(r"<[^>]+>", " ", body)
            parts.append(body)

        return unescape("\n".join(part for part in parts if part).strip())

    def _extract_mail_fields(self, mail: Dict[str, Any]) -> Dict[str, str]:
        """统一提取邮件字段，兼容 raw MIME 和不同 Worker 返回格式。"""
        sender = str(
            mail.get("source")
            or mail.get("from")
            or mail.get("from_address")
            or mail.get("fromAddress")
            or ""
        ).strip()
        subject = str(mail.get("subject") or mail.get("title") or "").strip()
        body_text = str(
            mail.get("text")
            or mail.get("body")
            or mail.get("content")
            or mail.get("html")
            or ""
        ).strip()
        raw = str(mail.get("raw") or "").strip()

        if raw:
            try:
                message = message_from_string(raw, policy=email_policy)
                sender = sender or self._decode_mime_header(message.get("From", ""))
                subject = subject or self._decode_mime_header(message.get("Subject", ""))
                parsed_body = self._extract_body_from_message(message)
                if parsed_body:
                    body_text = f"{body_text}\n{parsed_body}".strip() if body_text else parsed_body
            except Exception as e:
                logger.debug(f"解析 TempMail raw 邮件失败: {e}")
                body_text = f"{body_text}\n{raw}".strip() if body_text else raw

        body_text = unescape(re.sub(r"<[^>]+>", " ", body_text))
        return {
            "sender": sender,
            "subject": subject,
            "body": body_text,
            "raw": raw,
        }

    def _admin_headers(self) -> Dict[str, str]:
        """构造 admin 请求头"""
        return {
            "x-admin-auth": self.config["admin_password"],
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _make_request(self, method: str, path: str, **kwargs) -> Any:
        """
        发送请求并返回 JSON 数据

        Args:
            method: HTTP 方法
            path: 请求路径（以 / 开头）
            **kwargs: 传递给 http_client.request 的额外参数

        Returns:
            响应 JSON 数据

        Raises:
            EmailServiceError: 请求失败
        """
        base_url = self.config["base_url"].rstrip("/")
        url = f"{base_url}{path}"

        # 合并默认 admin headers
        kwargs.setdefault("headers", {})
        for k, v in self._admin_headers().items():
            kwargs["headers"].setdefault(k, v)

        try:
            response = self.http_client.request(method, url, **kwargs)

            if response.status_code >= 400:
                error_msg = f"请求失败: {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg = f"{error_msg} - {error_data}"
                except Exception:
                    error_msg = f"{error_msg} - {response.text[:200]}"
                self.update_status(False, EmailServiceError(error_msg))
                raise EmailServiceError(error_msg)

            try:
                return response.json()
            except json.JSONDecodeError:
                return {"raw_response": response.text}

        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"请求失败: {method} {path} - {e}")

    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        通过 admin API 创建临时邮箱

        Returns:
            包含邮箱信息的字典:
            - email: 邮箱地址
            - jwt: 用户级 JWT token
            - service_id: 同 email（用作标识）
        """
        import random
        import string

        # 生成随机邮箱名
        letters = ''.join(random.choices(string.ascii_lowercase, k=5))
        digits = ''.join(random.choices(string.digits, k=random.randint(1, 3)))
        suffix = ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 3)))
        name = letters + digits + suffix

        domain = self.config["domain"]
        enable_prefix = self.config.get("enable_prefix", True)

        body = {
            "enablePrefix": enable_prefix,
            "name": name,
            "domain": domain,
        }

        try:
            response = self._make_request("POST", "/admin/new_address", json=body)

            address = response.get("address", "").strip()
            jwt = response.get("jwt", "").strip()

            if not address:
                raise EmailServiceError(f"API 返回数据不完整: {response}")

            email_info = {
                "email": address,
                "jwt": jwt,
                "service_id": address,
                "id": address,
                "created_at": time.time(),
            }

            # 缓存 jwt，供获取验证码时使用
            self._email_cache[address] = email_info

            logger.info(f"成功创建 TempMail 邮箱: {address}")
            self.update_status(True)
            return email_info

        except Exception as e:
            self.update_status(False, e)
            if isinstance(e, EmailServiceError):
                raise
            raise EmailServiceError(f"创建邮箱失败: {e}")

    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = OTP_CODE_PATTERN,
        otp_sent_at: Optional[float] = None,
    ) -> Optional[str]:
        """
        从 TempMail 邮箱获取验证码

        Args:
            email: 邮箱地址
            email_id: 未使用，保留接口兼容
            timeout: 超时时间（秒）
            pattern: 验证码正则
            otp_sent_at: OTP 发送时间戳，用于过滤旧邮件

        Returns:
            验证码字符串，超时返回 None
        """
        logger.info(f"正在从 TempMail 邮箱 {email} 获取验证码...")

        start_time = time.time()
        seen_mail_ids: set = set()  # 已确认无需再次处理的邮件
        mail_attempts: Dict[str, int] = {}  # 允许同一封邮件重复解析（内容可能延迟填充）
        max_attempts_per_mail = 3

        # 优先使用用户级 JWT，回退到 admin API
        cached = self._email_cache.get(email, {})
        jwt = cached.get("jwt")

        normalized_email = str(email or "").strip().lower()
        used_codes = self._used_codes.setdefault(normalized_email, set())

        def parse_created_at(value: Any, reference_ts: Optional[float] = None) -> Optional[float]:
            if value is None:
                return None
            if isinstance(value, (int, float)):
                try:
                    return float(value)
                except Exception:
                    return None
            text = str(value).strip()
            if not text:
                return None
            # 支持纯数字时间戳（秒 / 毫秒）
            if text.isdigit():
                try:
                    num = float(text)
                    if num > 10_000_000_000:  # 13 位毫秒级
                        num = num / 1000.0
                    return num
                except Exception:
                    return None
            try:
                normalized = text.replace("Z", "+00:00")
                dt = datetime.fromisoformat(normalized)

                # 1) 有时区：直接转 UTC
                if dt.tzinfo is not None:
                    return dt.astimezone(timezone.utc).timestamp()

                # 2) 无时区：同时按 UTC / 本地时区解释，选择更接近 reference_ts 的那个
                local_tz = datetime.now().astimezone().tzinfo or timezone.utc
                candidates = [
                    dt.replace(tzinfo=timezone.utc).timestamp(),
                    dt.replace(tzinfo=local_tz).timestamp(),
                ]
                if reference_ts:
                    return min(candidates, key=lambda ts: abs(ts - reference_ts))
                return candidates[1]
            except Exception:
                pass

            # 兼容无时区字符串（例如 UI 中看到的 "2026/3/24 14:52:18"）
            for fmt in ("%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M", "%Y-%m-%d %H:%M"):
                try:
                    dt = datetime.strptime(text, fmt)
                    local_tz = datetime.now().astimezone().tzinfo or timezone.utc
                    candidates = [
                        dt.replace(tzinfo=timezone.utc).timestamp(),
                        dt.replace(tzinfo=local_tz).timestamp(),
                    ]
                    if reference_ts:
                        return min(candidates, key=lambda ts: abs(ts - reference_ts))
                    return candidates[1]
                except Exception:
                    continue

            return None

        min_timestamp = (otp_sent_at - 60) if otp_sent_at else 0
        if otp_sent_at:
            try:
                otp_human = datetime.fromtimestamp(otp_sent_at, tz=timezone.utc).isoformat()
            except Exception:
                otp_human = str(otp_sent_at)
            # 诊断日志（默认不输出；需要时将日志级别调到 DEBUG）
            logger.debug(
                f"[{email}] 本次 OTP 发送时间戳: {otp_sent_at:.3f} ({otp_human})，将跳过早于 {min_timestamp:.3f} 的旧邮件"
            )

        while time.time() - start_time < timeout:
            try:
                # if jwt:
                #     response = self._make_request(
                #         "GET",
                #         "/user_api/mails",
                #         params={"limit": 20, "offset": 0},
                #         headers={"x-user-token": jwt, "Content-Type": "application/json", "Accept": "application/json"},
                #     )
                # else:
                #     response = self._make_request(
                #         "GET",
                #         "/admin/mails",
                #         params={"limit": 20, "offset": 0, "address": email},
                #     )
                # 走 user 通道无法获取邮件,如果通过 try catch 走 admin 通道,会因为之前走过 user 通道,导致失效的问题
                response = self._make_request(
                    "GET",
                    "/admin/mails",
                    params={"limit": 20, "offset": 0, "address": email},
                )

                # /user_api/mails 和 /admin/mails 返回格式相同: {"results": [...], "total": N}
                mails = response.get("results", [])
                if not isinstance(mails, list):
                    time.sleep(3)
                    continue

                # 旧实现（保留注释便于参考）：按 API 原顺序扫描，命中后直接返回，可能取到旧邮件验证码
                # for mail in mails:
                #     ...

                # 优先扫描“更近的邮件”，避免 API 返回顺序不稳定导致先命中旧验证码
                # 没有 createdAt 的邮件放到最后，减少误判风险（并且只解析 createdAt 一次）
                mail_items: List[Dict[str, Any]] = []
                for mail in mails:
                    created_at_raw = mail.get("createdAt") or mail.get("created_at")
                    created_ts = parse_created_at(created_at_raw, otp_sent_at)
                    mail_items.append(
                        {
                            "mail": mail,
                            "id": mail.get("id"),
                            "created_at_raw": created_at_raw,
                            "created_ts": created_ts,
                        }
                    )

                mail_items.sort(
                    key=lambda item: (1 if item["created_ts"] is not None else 0, item["created_ts"] or 0),
                    reverse=True,
                )

                # 诊断日志：每轮轮询打印前几封邮件的解析结果（默认不输出；需要时将日志级别调到 DEBUG）
                logger.debug(f"[{email}] 拉取到 {len(mail_items)} 封邮件，min_timestamp={min_timestamp:.3f}")
                for idx, item in enumerate(mail_items[:3]):
                    mid = item.get("id")
                    cat_raw = item.get("created_at_raw")
                    cat_ts = item.get("created_ts")
                    delta = (cat_ts - otp_sent_at) if (cat_ts is not None and otp_sent_at) else None
                    m = item.get("mail") or {}
                    subj = (m.get("subject") or m.get("title") or "")
                    src = (m.get("source") or m.get("from") or m.get("from_address") or m.get("fromAddress") or "")
                    logger.debug(
                        f"[{email}] 预览[{idx}] id={mid}, createdAt={cat_raw!r}, ts={cat_ts}, Δ={delta}, from={str(src)[:60]!r}, subject={str(subj)[:80]!r}"
                    )

                for item in mail_items:
                    mail = item["mail"]
                    mail_id = item["id"]

                    # 旧实现（保留注释便于参考）：seen 后不再解析
                    # if not mail_id or mail_id in seen_mail_ids:
                    #     continue

                    if not mail_id:
                        logger.debug(f"[{email}] 跳过无 id 邮件: subject={(mail.get('subject') or '')[:60]!r}")
                        continue
                    if mail_id in seen_mail_ids:
                        continue

                    created_at_raw = item.get("created_at_raw")
                    created_timestamp = item.get("created_ts")
                    if min_timestamp and created_timestamp is not None and created_timestamp < min_timestamp:
                        seen_mail_ids.add(mail_id)
                        logger.debug(
                            f"[{email}] 跳过旧邮件 id={mail_id}, createdAt={created_at_raw}, subject={(mail.get('subject') or '')[:60]!r}"
                        )
                        continue

                    # 旧实现（保留注释便于参考）：进入解析前就标记为 seen，可能导致“内容未完整时错过验证码”
                    # seen_mail_ids.add(mail_id)

                    parsed = self._extract_mail_fields(mail)
                    sender = parsed["sender"].lower()
                    subject = parsed["subject"]
                    body_text = parsed["body"]
                    raw_text = parsed["raw"]
                    content = f"{sender}\n{subject}\n{body_text}\n{raw_text}".strip()

                    # 邮件内容可能延迟填充：对“内容不足”的邮件不要立刻拉黑，允许后续轮询再次解析
                    attempt_count = mail_attempts.get(mail_id, 0) + 1
                    mail_attempts[mail_id] = attempt_count
                    if len(content) < 40 and attempt_count <= max_attempts_per_mail:
                        logger.debug(
                            f"[{email}] 邮件内容过短，稍后重试 id={mail_id}, attempt={attempt_count}/{max_attempts_per_mail}, createdAt={created_at_raw!r}, subject={subject[:80]!r}"
                        )
                        continue

                    content_lower = content.lower()

                    # 只处理 OpenAI / ChatGPT 验证相关邮件
                    # 说明：部分邮件正文/主题可能不包含 "openai" 字样（例如 "The ChatGPT team"），但仍是 OpenAI 的 OTP 邮件
                    is_target = (
                        ("openai" in sender)
                        or ("openai" in content_lower)
                        or (
                            "chatgpt" in content_lower
                            and any(
                                kw in content_lower
                                for kw in (
                                    "verification code",
                                    "temporary verification code",
                                    "log-in code",
                                    "one-time password",
                                    "otp",
                                )
                            )
                        )
                    )

                    if not is_target:
                        # raw 可能稍后才填充（From/Headers 会补齐），先重试几轮再判定为非目标邮件
                        if not raw_text and attempt_count <= max_attempts_per_mail:
                            logger.debug(
                                f"[{email}] 非目标邮件(可能未完整)，稍后重试 id={mail_id}, attempt={attempt_count}/{max_attempts_per_mail}, createdAt={created_at_raw!r}, subject={subject[:80]!r}"
                            )
                            continue

                        seen_mail_ids.add(mail_id)
                        logger.debug(
                            f"[{email}] 跳过非目标邮件 id={mail_id}, createdAt={created_at_raw!r}, subject={subject[:80]!r}"
                        )
                        continue

                    match = re.search(pattern, content)
                    if match:
                        code = match.group(1)
                        if code in used_codes:
                            # 已用过的 code 直接跳过，继续等待新邮件/新验证码
                            seen_mail_ids.add(mail_id)
                            logger.debug(
                                f"[{email}] 跳过已使用验证码: {code} (mail_id={mail_id}, createdAt={created_at_raw!r}, subject={subject[:80]!r})"
                            )
                            continue

                        used_codes.add(code)
                        logger.info(
                            f"[{email}] 从 TempMail 找到验证码: {code} (mail_id={mail_id}, createdAt={created_at_raw}, subject={subject[:80]!r})"
                        )
                        self.update_status(True)
                        return code

                    # 没匹配到验证码：若内容已较充分或已重试多次，则标记为已处理，避免无效重复解析
                    if len(content) >= 200 or attempt_count >= max_attempts_per_mail:
                        seen_mail_ids.add(mail_id)
                        logger.debug(
                            f"[{email}] 未匹配到验证码，标记为已处理 id={mail_id}, attempt={attempt_count}, createdAt={created_at_raw!r}, subject={subject[:80]!r}"
                        )

            except Exception as e:
                logger.warning(f"检查 TempMail 邮件时出错: {e}")

            time.sleep(3)

        logger.warning(f"等待 TempMail 验证码超时: {email}")
        return None

    def list_emails(self, limit: int = 100, offset: int = 0, **kwargs) -> List[Dict[str, Any]]:
        """
        列出邮箱

        Args:
            limit: 返回数量上限
            offset: 分页偏移
            **kwargs: 额外查询参数，透传给 admin API

        Returns:
            邮箱列表
        """
        params = {
            "limit": limit,
            "offset": offset,
        }
        params.update({k: v for k, v in kwargs.items() if v is not None})

        try:
            response = self._make_request("GET", "/admin/mails", params=params)
            mails = response.get("results", [])
            if not isinstance(mails, list):
                raise EmailServiceError(f"API 返回数据格式错误: {response}")

            emails: List[Dict[str, Any]] = []
            for mail in mails:
                address = (mail.get("address") or "").strip()
                mail_id = mail.get("id") or address
                email_info = {
                    "id": mail_id,
                    "service_id": mail_id,
                    "email": address,
                    "subject": mail.get("subject"),
                    "from": mail.get("source"),
                    "created_at": mail.get("createdAt") or mail.get("created_at"),
                    "raw_data": mail,
                }
                emails.append(email_info)

                if address:
                    cached = self._email_cache.get(address, {})
                    self._email_cache[address] = {**cached, **email_info}

            self.update_status(True)
            return emails
        except Exception as e:
            logger.warning(f"列出 TempMail 邮箱失败: {e}")
            self.update_status(False, e)
            return list(self._email_cache.values())

    def delete_email(self, email_id: str) -> bool:
        """
        删除邮箱

        Note:
            当前 TempMail admin API 文档未见删除地址接口，这里先从本地缓存移除，
            以满足统一接口并避免服务实例化失败。
        """
        removed = False
        emails_to_delete = []

        for address, info in self._email_cache.items():
            candidate_ids = {
                address,
                info.get("id"),
                info.get("service_id"),
            }
            if email_id in candidate_ids:
                emails_to_delete.append(address)

        for address in emails_to_delete:
            self._email_cache.pop(address, None)
            removed = True

        if removed:
            logger.info(f"已从 TempMail 缓存移除邮箱: {email_id}")
            self.update_status(True)
        else:
            logger.info(f"TempMail 缓存中未找到邮箱: {email_id}")

        return removed

    def check_health(self) -> bool:
        """检查服务健康状态"""
        try:
            self._make_request(
                "GET",
                "/admin/mails",
                params={"limit": 1, "offset": 0},
            )
            self.update_status(True)
            return True
        except Exception as e:
            logger.warning(f"TempMail 健康检查失败: {e}")
            self.update_status(False, e)
            return False
