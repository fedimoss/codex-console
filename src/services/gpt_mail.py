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

    def __init__(self, config: Dict[str, Any] = None, name: str = None):
        super().__init__(EmailServiceType.GPT_MAIL, name)

        cfg = config or {}
        required_keys = ["base_url", "api_key"]
        missing_keys = [key for key in required_keys if not cfg.get(key)]
        if missing_keys:
            raise ValueError(f"Missing required config keys: {missing_keys}")

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

    def _headers(self) -> Dict[str, str]:
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-API-Key": str(self.config["api_key"]),
        }

    def _make_request(self, method: str, path: str, **kwargs) -> Any:
        url = f"{self.config['base_url']}{path}"
        headers = dict(self._headers())
        headers.update(kwargs.pop("headers", {}) or {})
        kwargs["headers"] = headers

        try:
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

    def _unwrap_data(self, payload: Any) -> Any:
        if isinstance(payload, dict) and "data" in payload:
            return payload.get("data")
        return payload

    def _extract_email(self, payload: Any) -> str:
        data = self._unwrap_data(payload)
        if isinstance(data, dict):
            return str(data.get("email") or data.get("address") or "").strip()
        return ""

    def _extract_mail_list(self, payload: Any) -> List[Dict[str, Any]]:
        data = self._unwrap_data(payload)
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)]
        if isinstance(data, dict):
            for key in ("emails", "messages", "items", "results", "list"):
                value = data.get(key)
                if isinstance(value, list):
                    return [item for item in value if isinstance(item, dict)]
        return []

    def _extract_mail_detail(self, payload: Any) -> Dict[str, Any]:
        data = self._unwrap_data(payload)
        if isinstance(data, dict):
            for key in ("email", "message", "item"):
                value = data.get(key)
                if isinstance(value, dict):
                    return value
            return data
        return {}

    def _extract_mail_id(self, mail: Dict[str, Any]) -> str:
        for key in ("id", "email_id", "mail_id", "mailId", "_id", "uuid"):
            value = mail.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    def _generate_local_part(self) -> str:
        first = random.choice(string.ascii_lowercase)
        rest = "".join(random.choices(string.ascii_lowercase + string.digits, k=7))
        return f"{first}{rest}"

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

    def _extract_mail_timestamp(self, mail: Dict[str, Any]) -> Optional[float]:
        for key in ("created_at", "createdAt", "timestamp", "date", "received_at", "receivedAt"):
            ts = self._parse_timestamp(mail.get(key))
            if ts is not None:
                return ts
        return None

    def _extract_mail_text(self, mail: Dict[str, Any]) -> Dict[str, str]:
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

    def _is_openai_otp_mail(self, sender: str, subject: str, body: str) -> bool:
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

    def _extract_otp(self, content: str, pattern: str) -> Optional[str]:
        semantic_match = re.search(OTP_CODE_SEMANTIC_PATTERN, content, re.IGNORECASE)
        if semantic_match:
            return semantic_match.group(1)

        simple_match = re.search(pattern, content)
        if simple_match:
            return simple_match.group(1)
        return None

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
            response = self._make_request("POST", "/api/generate-email", json={"prefix": prefix})
        else:
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
        self._email_cache[email] = email_info
        self.update_status(True)
        logger.info("Created GPTMail address: %s", email)
        return email_info

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

        while time.time() - start_time < timeout:
            try:
                response = self._make_request("GET", "/api/emails", params={"email": email})
                mails = self._extract_mail_list(response)
                mails = sorted(
                    mails,
                    key=lambda item: self._extract_mail_timestamp(item) or 0.0,
                    reverse=True,
                )

                for mail in mails:
                    mail_id = self._extract_mail_id(mail)
                    if not mail_id or mail_id in seen_mail_ids or mail_id == last_used_mail_id:
                        continue

                    seen_mail_ids.add(mail_id)

                    mail_ts = self._extract_mail_timestamp(mail)
                    if otp_sent_at and mail_ts is not None and mail_ts + 2 < otp_sent_at:
                        continue

                    detail_payload = self._make_request("GET", f"/api/email/{mail_id}")
                    detail = self._extract_mail_detail(detail_payload)
                    parsed = self._extract_mail_text({**mail, **detail})
                    sender = parsed["sender"]
                    subject = parsed["subject"]
                    body = parsed["body"]

                    if not self._is_openai_otp_mail(sender, subject, body):
                        continue

                    code = self._extract_otp(
                        "\n".join([sender, subject, body]).strip(),
                        pattern,
                    )
                    if not code:
                        continue

                    self._last_used_mail_ids[email] = mail_id
                    self.update_status(True)
                    logger.info("Found GPTMail verification code for %s", email)
                    return code
            except Exception as exc:
                logger.debug("GPTMail poll failed: %s", exc)

            time.sleep(3)

        logger.warning("Timed out waiting for GPTMail code: %s", email)
        return None

    def list_emails(self, **kwargs) -> List[Dict[str, Any]]:
        return list(self._email_cache.values())

    def delete_email(self, email_id: str) -> bool:
        email_addr = str(email_id or "").strip()
        if not email_addr:
            return False

        try:
            self._make_request("DELETE", "/api/emails/clear", params={"email": email_addr})
            self._email_cache.pop(email_addr, None)
            self._last_used_mail_ids.pop(email_addr, None)
            self.update_status(True)
            return True
        except Exception as exc:
            logger.warning("Failed to clear GPTMail inbox: %s", exc)
            self.update_status(False, exc)
            return False

    def check_health(self) -> bool:
        email_addr = ""
        try:
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

    def get_service_info(self) -> Dict[str, Any]:
        return {
            "service_type": self.service_type.value,
            "name": self.name,
            "base_url": self.config["base_url"],
            "has_domain": bool(self.config.get("domain")),
            "cached_emails": len(self._email_cache),
            "status": self.status.value,
        }
