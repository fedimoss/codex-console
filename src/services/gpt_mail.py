"""
GPTMail email service integration.

Supports two creation modes:
- no configured domain: ask the upstream API to generate an address
- configured domain: build a local address directly with that domain
"""

import logging
import random
import re
import string
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any, Dict, List, Optional

from .base import BaseEmailService, EmailServiceError, EmailServiceType
from ..config.constants import OTP_CODE_PATTERN, OTP_CODE_SEMANTIC_PATTERN
from ..core.http_client import HTTPClient, RequestConfig


logger = logging.getLogger(__name__)


class GPTMailService(BaseEmailService):
    """GPTMail email service."""

    # 这份服务的主线很简单：先拿到一个邮箱地址，再持续轮询收件箱，最后从目标邮件里提取验证码。
    # 初始化服务配置、HTTP 客户端和运行时缓存。
    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        super().__init__(EmailServiceType.GPT_MAIL, name)

        cfg = config or {}
        required_keys = ["base_url", "api_key"]
        missing_keys = [key for key in required_keys if not cfg.get(key)]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {missing_keys}")

        # 先补齐默认配置，再统一清洗地址和域名，避免后面每次请求都重复处理。
        default_config = {
            "domain": "",
            "timeout": 30,
            "max_retries": 3,
            "proxy_url": None,
        }
        self.config = {**default_config, **cfg}
        self.config["base_url"] = str(self.config["base_url"]).rstrip("/")
        self.config["domain"] = str(self.config.get("domain") or "").strip().lstrip("@")

        http_config = RequestConfig(
            timeout=int(self.config["timeout"]),
            max_retries=int(self.config["max_retries"]),
        )
        self.http_client = HTTPClient(
            proxy_url=self.config.get("proxy_url"),
            config=http_config,
        )

        self._email_cache: Dict[str, Dict[str, Any]] = {}
        self._last_used_mail_ids: Dict[str, str] = {}

    # 生成请求 GPTMail 接口时统一使用的请求头。
    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-Key": str(self.config["api_key"]),
        }

    # 统一发起接口请求，并把返回结果和异常整理成固定格式。
    def _make_request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self.config['base_url']}{path}"
        headers = dict(self._headers())
        headers.update(kwargs.pop("headers", {}) or {})
        kwargs["headers"] = headers

        try:
            # 统一在这里补请求头、抛业务异常，外层方法只关心拿到的结果能不能用。
            response = self.http_client.request(method, url, **kwargs)
            if response.status_code >= 400:
                error_message = f"Request failed: {response.status_code}"
                try:
                    payload = response.json()
                    error_message = f"{error_message} - {payload}"
                except Exception:
                    error_message = f"{error_message} - {response.text[:200]}"
                raise EmailServiceError(error_message)

            try:
                payload = response.json()
            except Exception:
                payload = {"raw_response": response.text}

            if isinstance(payload, dict) and payload.get("success") is False:
                raise EmailServiceError(str(payload))

            return payload
        except Exception as exc:
            self.update_status(False, exc)
            if isinstance(exc, EmailServiceError):
                raise
            raise EmailServiceError(f"Request failed: {method} {path} - {exc}")

    # 取出接口返回里的核心数据，兼容带 data 和不带 data 的两种结构。
    def _unwrap_data(self, payload: Any) -> Any:
        # GPTMail 的返回外层有时包在 data 里，有时直接给结果，这里先抹平结构差异。
        if isinstance(payload, dict) and "data" in payload:
            return payload.get("data")
        return payload

    # 从建邮箱接口返回里提取邮箱地址。
    def _extract_email(self, payload: Any) -> str:
        data = self._unwrap_data(payload)
        if isinstance(data, dict):
            return str(data.get("email") or data.get("address") or "").strip()
        return ""

    # 从收件箱列表接口返回里提取邮件列表。
    def _extract_mail_list(self, payload: Any) -> List[Dict[str, Any]]:
        data = self._unwrap_data(payload)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            # 上游接口字段名不稳定，这里把常见列表字段都兼容掉，减少外层判断分支。
            for key in ("emails", "messages", "items", "results", "list"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    # 从单封邮件详情接口返回里提取具体邮件内容。
    def _extract_mail_detail(self, payload: Any) -> Dict[str, Any]:
        data = self._unwrap_data(payload)
        if isinstance(data, dict):
            # 详情接口也可能再包一层对象，这里统一拆开，方便和列表数据合并使用。
            for key in ("email", "message", "item"):
                value = data.get(key)
                if isinstance(value, dict):
                    return value
            return data
        return {}

    # 从单封邮件数据里提取邮件唯一标识。
    def _extract_mail_id(self, mail: Dict[str, Any]) -> str:
        for key in ("id", "email_id", "mail_id", "mailId", "_id", "uuid"):
            value = mail.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    # 生成一个本地可用的邮箱前缀。
    def _generate_local_part(self) -> str:
        # 本地域名模式下，本地直接拼一个可用邮箱前缀，减少一次远程调用。
        first = random.choice(string.ascii_lowercase)
        rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=7))
        return f"{first}{rest}"

    # 把各种格式的时间值统一转成时间戳。
    def _parse_timestamp(self, value: Any) -> Optional[float]:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            ts = float(value)
            if ts > 10**12:
                ts /= 1000.0
            return ts if ts > 0 else None

        text = str(value).strip()
        if not text:
            return None

        # 接口时间格式不固定，这里兼容时间戳、ISO 字符串和邮件头日期。
        if text.isdigit():
            ts = float(text)
            if ts > 10**12:
                ts /= 1000.0
            return ts if ts > 0 else None

        iso_text = text[:-1] + "+00:00" if text.endswith("Z") else text
        try:
            dt = datetime.fromisoformat(iso_text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            pass

        try:
            dt = parsedate_to_datetime(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return None

    # 从邮件数据里挑出最合适的收信时间。
    def _extract_mail_timestamp(self, mail: Dict[str, Any]) -> Optional[float]:
        for key in ("created_at", "createdAt", "timestamp", "date", "received_at", "receivedAt"):
            ts = self._parse_timestamp(mail.get(key))
            if ts is not None:
                return ts
        return None

    # 把邮件发件人、标题和正文整理成统一文本。
    def _extract_mail_text(self, mail: Dict[str, Any]) -> Dict[str, str]:
        # 不同接口字段名不一致，正文还可能混着 HTML，这里统一整理成可搜索文本。
        sender = str(
            mail.get("from")
            or mail.get("sender")
            or mail.get("from_address")
            or mail.get("fromAddress")
            or ""
        ).strip()
        subject = str(mail.get("subject") or mail.get("title") or "").strip()
        text_body = str(
            mail.get("text")
            or mail.get("plain")
            or mail.get("body")
            or mail.get("content")
            or ""
        ).strip()
        html_body = str(
            mail.get("html")
            or mail.get("html_content")
            or mail.get("htmlContent")
            or ""
        ).strip()

        if html_body:
            html_body = re.sub(r"<[^>]+>", " ", html_body)
        body = "\n".join(part for part in (text_body, html_body) if part).strip()
        body = unescape(body)

        return {
            "sender": sender,
            "subject": subject,
            "body": body,
        }

    # 判断一封邮件像不像 OpenAI 发来的验证码邮件。
    def _is_openai_otp_mail(self, sender: str, subject: str, body: str) -> bool:
        # 先粗筛是不是 OpenAI 的验证码邮件，避免把别的业务邮件误识别成验证码来源。
        blob = "\n".join([str(sender or ""), str(subject or ""), str(body or "")]).lower()
        if "openai" not in blob:
            return False

        otp_keywords = (
            "verification code",
            "one-time code",
            "one time code",
            "security code",
            "code is",
            "verify",
            "otp",
            "login",
            "log in",
            "验证码",
        )
        return any(keyword in blob for keyword in otp_keywords)

    # 从邮件文本里提取验证码。
    def _extract_otp(self, content: str, pattern: str) -> Optional[str]:
        # 优先用语义更强的规则提取，兜底再走普通数字匹配，减少误抓正文里的其他数字。
        semantic_match = re.search(OTP_CODE_SEMANTIC_PATTERN, content, re.IGNORECASE)
        if semantic_match:
            return semantic_match.group(1)

        simple_match = re.search(pattern, content)
        if simple_match:
            return simple_match.group(1)
        return None

    # 创建一个可用邮箱，按配置决定本地拼接还是远程生成。
    def create_email(self, config: Dict[str, Any] = None) -> Dict[str, Any]:
        request_config = config or {}
        domain = str(
            request_config.get("domain")
            or request_config.get("default_domain")
            or self.config.get("domain")
            or ""
        ).strip().lstrip("@")
        prefix = str(
            request_config.get("prefix")
            or request_config.get("name")
            or ""
        ).strip()

        # 业务上优先复用固定域名，因为这样更快、更可控，也不依赖上游生成地址。
        # 有固定域名就本地直接拼邮箱；没有域名时再调用 GPTMail 生成随机地址。
        if domain:
            local_part = prefix or self._generate_local_part()
            response = {
                "success": True,
                "data": {
                    "email": f"{local_part}@{domain}",
                },
                "source": "local-domain",
            }
        elif prefix:
            # 传了前缀时，说明业务希望邮箱名尽量可读，这时交给上游按指定前缀生成。
            response = self._make_request("POST", "/api/generate-email", json={"prefix": prefix})
        else:
            # 什么都没指定时，直接拿一个随机邮箱，适合临时注册或验证码场景。
            response = self._make_request("GET", "/api/generate-email")

        email = self._extract_email(response)
        if not email:
            raise EmailServiceError(f"Create email failed, no email returned: {response}")

        email_info = {
            "email": email,
            "service_id": email,
            "id": email,
            "domain": domain or str(email).split("@")[-1],
            "created_at": time.time(),
            "raw_response": response,
        }
        # 缓存创建结果，后续列出邮箱、健康检查和删除时都能复用这份信息。
        self._email_cache[email] = email_info
        self.update_status(True)
        logger.info("Created GPTMail address: %s", email)
        return email_info

    # 轮询指定邮箱，找到最新可用的验证码并返回。
    def get_verification_code(
        self,
        email: str,
        email_id: str = None,
        timeout: int = 120,
        pattern: str = OTP_CODE_PATTERN,
        otp_sent_at: Optional[float] = None,
    ) -> Optional[str]:
        start_time = time.time()
        seen_mail_ids = set()
        last_used_mail_id = self._last_used_mail_ids.get(email)

        # 业务目标不是“拿到任意一封邮件”，而是“拿到这次流程刚产生的最新验证码”。
        # 轮询收件箱直到超时，只处理本次还没看过的新邮件。
        while time.time() - start_time < timeout:
            try:
                response = self._make_request("GET", "/api/emails", params={"email": email})
                mails = self._extract_mail_list(response)
                mails = sorted(
                    mails,
                    key=lambda item: self._extract_mail_timestamp(item) or 0.0,
                    reverse=True,
                )

                # 先按时间倒序处理，优先检查最新邮件，能更快命中刚收到的验证码。
                for mail in mails:
                    mail_id = self._extract_mail_id(mail)
                    # 跳过无效邮件、这轮已经看过的邮件，以及上一次已经成功用过的邮件。
                    if not mail_id or mail_id in seen_mail_ids or mail_id == last_used_mail_id:
                        continue

                    seen_mail_ids.add(mail_id)

                    mail_ts = self._extract_mail_timestamp(mail)
                    # 如果业务方记录了验证码发送时间，就跳过明显更早的旧邮件。
                    if otp_sent_at and mail_ts is not None and mail_ts + 2 < otp_sent_at:
                        continue

                    detail_payload = self._make_request("GET", f"/api/email/{mail_id}")
                    detail = self._extract_mail_detail(detail_payload)
                    # 列表页信息通常不全，把详情补齐后再统一做正文解析和验证码识别。
                    parsed = self._extract_mail_text({**mail, **detail})
                    sender = parsed["sender"]
                    subject = parsed["subject"]
                    body = parsed["body"]

                    # 先判断是不是目标业务邮件，避免普通通知邮件里的数字被误当成验证码。
                    if not self._is_openai_otp_mail(sender, subject, body):
                        continue

                    code = self._extract_otp(
                        "\n".join([sender, subject, body]).strip(),
                        pattern,
                    )
                    if not code:
                        # 有些邮件会命中关键词但正文里没有有效验证码，这种情况继续找下一封。
                        continue

                    # 记住这封已使用邮件，下一次取码时不要重复返回旧验证码。
                    self._last_used_mail_ids[email] = mail_id
                    self.update_status(True)
                    logger.info("Found GPTMail verification code for %s", email)
                    return code
            except Exception as exc:
                # 轮询阶段允许短暂失败，避免一次接口抖动就让整个取码流程提前终止。
                logger.debug("GPTMail poll failed: %s", exc)

            # 上游接口是轮询式收信，短暂等待可以避免高频请求把服务打满。
            time.sleep(3)

        logger.warning("Timed out waiting for GPTMail code: %s", email)
        return None

    # 返回当前服务里缓存过的邮箱信息。
    def list_emails(self, **kwargs) -> List[Dict[str, Any]]:
        # 这里只返回本进程创建过的邮箱，不会反查远端所有历史邮箱。
        return list(self._email_cache.values())

    # 清空指定邮箱地址的收件箱，并清理本地缓存。
    def delete_email(self, email_id: str) -> bool:
        email_addr = str(email_id or "").strip()
        if not email_addr:
            return False

        try:
            # GPTMail 这里清的是这个地址的收件箱，不是删除账号本身。
            self._make_request("DELETE", "/api/emails/clear", params={"email": email_addr})
            self._email_cache.pop(email_addr, None)
            self._last_used_mail_ids.pop(email_addr, None)
            self.update_status(True)
            return True
        except Exception as exc:
            logger.warning("Failed to clear GPTMail inbox: %s", exc)
            self.update_status(False, exc)
            return False

    # 用创建邮箱的结果检查这个服务当前是否可用。
    def check_health(self) -> bool:
        email_addr = ""
        try:
            # 健康检查只验证“能创建邮箱”这件事，成功后顺手清掉测试痕迹。
            created = self.create_email()
            email_addr = str(created.get("email") or "").strip()
            self.update_status(True)
            return True
        except Exception as exc:
            logger.warning("GPTMail health check failed: %s", exc)
            self.update_status(False, exc)
            return False
        finally:
            if email_addr:
                try:
                    self.delete_email(email_addr)
                except Exception:
                    pass

    # 返回这个服务的基础信息和当前状态。
    def get_service_info(self) -> Dict[str, Any]:
        return {
            "service_type": self.service_type.value,
            "name": self.name,
            "base_url": self.config["base_url"],
            "has_domain": bool(self.config.get("domain")),
            "cached_emails": len(self._email_cache),
            "status": self.status.value,
        }
