"""
PanSou 网盘搜索客户端
用于搜索各类网盘资源
"""
import re
import unicodedata
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any

import requests

from app.log import logger


class PanSouClient:
    """网盘搜索客户端"""

    # 网盘类型中文名映射
    TYPE_NAMES = {
        "baidu": "百度网盘",
        "aliyun": "阿里云盘",
        "quark": "夸克网盘",
        "tianyi": "天翼云盘",
        "uc": "UC网盘",
        "mobile": "移动云盘",
        "115": "115网盘",
        "pikpak": "PikPak",
        "xunlei": "迅雷云盘",
        "123": "123云盘",
        "magnet": "磁力链接",
        "ed2k": "电驴链接"
    }

    _PUNCT_GAP_RE = re.compile(r"[\s\u3000:：·•.,，。!！?？（）【】\[\]/／\\＼-]+")

    def __init__(
            self,
            base_url: str,
            username: str = "",
            password: str = "",
            auth_enabled: bool = True,
            proxy: str = None
    ):
        """
        初始化 PanSou 客户端

        :param base_url: API 基础地址
        :param username: 用户名
        :param password: 密码
        :param auth_enabled: 是否启用认证
        :param proxy: 代理地址，如 http://127.0.0.1:7890
        """
        self.base_url = base_url.rstrip("/") if base_url else ""
        self.username = username
        self.password = password
        self.auth_enabled = auth_enabled
        self._token: Optional[str] = None
        self._token_expires: Optional[datetime] = None
        # API 调用计数器
        self._api_call_count = 0
        # 代理设置（兼容字符串和字典格式）
        if proxy:
            self._proxies = proxy if isinstance(proxy, dict) else {"http": proxy, "https": proxy}
        else:
            self._proxies = None

    @staticmethod
    def _normalize_for_match(text: str) -> str:
        """
        统一空白、NFKC 与常见全角标点，便于做「关键词是否出现在标题中」的判断
        """
        if not text:
            return ""
        t = unicodedata.normalize("NFKC", text)
        for old, new in (
            ("：", ":"),
            ("，", ","),
            ("（", "("),
            ("）", ")"),
            ("【", "["),
            ("】", "]"),
            ("！", "!"),
            ("？", "?"),
            ("–", "-"),
            ("—", "-"),
            ("…", "..."),
        ):
            t = t.replace(old, new)
        t = re.sub(r"[\s\u3000]+", " ", t).strip()
        return t.casefold()

    @classmethod
    def _compact_for_match(cls, text: str) -> str:
        """
        在规范化基础上去掉标点与空白，使「复仇者联盟3：无限战争」与「复仇者联盟3: 无限战争」可比
        """
        base = cls._normalize_for_match(text)
        return cls._PUNCT_GAP_RE.sub("", base)

    @classmethod
    def _title_matches_search_key(cls, key: str, title: str) -> bool:
        """
        判断标题是否包含搜索关键词：先原串子串，再规范化子串，再紧凑子串（短关键词不用紧凑路径以免误伤）
        """
        if not key:
            return True
        t = title or ""
        if key in t:
            return True
        nk = cls._normalize_for_match(key)
        nt = cls._normalize_for_match(t)
        if nk and nk in nt:
            return True
        ck = cls._compact_for_match(key)
        ct = cls._compact_for_match(t)
        if len(ck) < 2:
            return False
        return ck in ct

    def _get_token(self) -> Optional[str]:
        """获取或刷新 Token"""
        if not self.base_url:
            return None

        if not self.auth_enabled:
            return None

        if not all([self.username, self.password]):
            logger.warning("PanSou 认证已启用但未配置用户名密码")
            return None

        # 检查 Token 是否有效（提前 5 分钟刷新）
        now = datetime.now()
        if self._token and self._token_expires:
            if now < self._token_expires - timedelta(minutes=5):
                return self._token

        # 登录获取新 Token
        try:
            login_url = f"{self.base_url}/api/auth/login"
            self._api_call_count += 1
            response = requests.post(
                login_url,
                json={"username": self.username, "password": self.password},
                timeout=10,
                proxies=self._proxies
            )

            if response.status_code == 200:
                data = response.json()
                self._token = data.get("token")
                expires_at = data.get("expires_at")
                if expires_at:
                    self._token_expires = datetime.fromtimestamp(expires_at)
                else:
                    self._token_expires = now + timedelta(hours=24)
                logger.debug("PanSou Token 获取成功")
                return self._token
            else:
                logger.error(f"PanSou 登录失败: HTTP {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"PanSou 登录失败: {e}")
            return None

    def search(
            self,
            keyword: str,
            cloud_types: List[str] = None,
            channels: List[str] = None,
            limit: int = 10
    ) -> Dict[str, Any]:
        """
        搜索网盘资源

        :param keyword: 搜索关键词
        :param cloud_types: 网盘类型列表，如 ["115", "quark"]
        :param channels: TG搜索频道列表 
        :param limit: 每种网盘类型返回的结果数量限制
        :return: 搜索结果
        """
        if not keyword or not keyword.strip():
            return {
                "error": "搜索关键词不能为空",
                "keyword": keyword
            }

        keyword = keyword.strip()

        if not self.base_url:
            return {
                "error": "未配置 PanSou API 地址",
                "keyword": keyword
            }

        try:
            limit = min(max(int(limit) if limit else 10, 1), 20)
        except (ValueError, TypeError):
            limit = 10

        try:
            headers = {"Content-Type": "application/json"}

            # 如果启用认证，获取 Token
            if self.auth_enabled:
                token = self._get_token()
                if not token:
                    return {
                        "error": "PanSou API 认证失败，请检查用户名和密码配置",
                        "keyword": keyword
                    }
                headers["Authorization"] = f"Bearer {token}"

            # 构建请求参数
            search_url = f"{self.base_url}/api/search"
            payload = {
                "kw": keyword,
                "refresh": True,
                "res": "merge"
            }
            if channels:
                payload["channels"] = channels

            if cloud_types:
                payload["cloud_types"] = cloud_types

            logger.info(f"PanSou 搜索: {payload}")

            # 带重试的搜索请求（规避接口临时性 HTTP 400 / 非 JSON 响应）
            result = self._api_search_with_retry(payload, headers, search_url, retries=3, limit=limit)

            # 指定频道无结果时，降级为全频道搜索（避免频道列表过时或覆盖不全导致漏检）
            if channels and result and not any(result.get("results", {}).values()):
                logger.info(f"PanSou 指定频道无结果，降级为全频道搜索: {keyword}")
                payload_without_channels = dict(payload)
                payload_without_channels.pop("channels", None)
                result = self._api_search_with_retry(
                    payload_without_channels, headers, search_url, retries=2, limit=limit
                )

            if result is None:
                return {
                    "error": f"搜索请求失败: PanSou 接口多次请求无有效响应",
                    "keyword": keyword
                }

            return {
                "keyword": keyword,
                "total": result.get("total", 0),
                "count": sum(len(v) for v in result.get("results", {}).values()),
                "results": result.get("results", {})
            }

        except requests.exceptions.Timeout:
            return {
                "error": "搜索请求超时，请稍后重试",
                "keyword": keyword
            }
        except Exception as e:
            logger.error(f"搜索网盘资源失败: {str(e)}")
            return {
                "error": f"搜索网盘资源失败: {str(e)}",
                "keyword": keyword
            }

    def _api_search_with_retry(
            self,
            payload: Dict[str, Any],
            headers: Dict[str, str],
            search_url: str,
            retries: int = 3,
            limit: int = 10
    ) -> Optional[Dict[str, Any]]:
        """
        发起 PanSou 搜索请求并带重试，返回解析后的结果（含 total 与 results）。

        规避现象：
        - HTTP 400 "The plain HTTP request was sent to HTTPS port"（源站反代配置问题，随机出现）
        - application/json 但正文无法被解析为 JSON
        - 突发 403/错误码响应

        :param payload: 请求体
        :param headers: 请求头
        :param search_url: 接口地址
        :param retries: 重试次数（默认 3，共尝试 4 次）
        :param limit: 每种类型的结果数限制
        :return: {"total": int, "results": dict}；全部失败返回 None
        """
        import time

        for attempt in range(retries + 1):
            try:
                self._api_call_count += 1
                response = requests.post(
                    search_url, json=payload, headers=headers, timeout=120, proxies=self._proxies
                )

                # Token 失效时刷新重试一次
                if response.status_code == 401 and self.auth_enabled:
                    self._token = None
                    self._token_expires = None
                    token = self._get_token()
                    if token:
                        headers["Authorization"] = f"Bearer {token}"
                        self._api_call_count += 1
                        response = requests.post(
                            search_url, json=payload, headers=headers, timeout=120, proxies=self._proxies
                        )

                if response.status_code != 200:
                    logger.warning(
                        f"PanSou 搜索非 200 (HTTP {response.status_code})，第 {attempt + 1}/{retries + 1} 次"
                    )
                    if attempt < retries:
                        time.sleep(2 * (attempt + 1))
                    continue

                try:
                    resp_data = response.json()
                except ValueError:
                    logger.warning(
                        f"PanSou 响应不是有效 JSON (len={len(response.content)})，第 {attempt + 1}/{retries + 1} 次"
                    )
                    if attempt < retries:
                        time.sleep(2 * (attempt + 1))
                    continue

                # 检查响应状态码
                if resp_data.get("code") != 0:
                    logger.warning(
                        f"PanSou 响应 code 非 0: {resp_data.get('message', '')}，第 {attempt + 1}/{retries + 1} 次"
                    )
                    if attempt < retries:
                        time.sleep(2 * (attempt + 1))
                    continue

                data = resp_data.get("data", {})
                total = data.get("total", 0)

                # 新版 API 返回 merged_by_type（按网盘类型聚合），旧版返回 results 列表
                merged_by_type = data.get("merged_by_type")
                if isinstance(merged_by_type, dict) and merged_by_type:
                    grouped_results = self._parse_merged(merged_by_type, payload.get("kw", ""), limit)
                else:
                    grouped_results = self._parse_results(data.get("results", []), payload.get("kw", ""), limit)

                return {
                    "total": total,
                    "results": grouped_results
                }

            except requests.exceptions.Timeout:
                logger.warning(f"PanSou 搜索请求超时，第 {attempt + 1}/{retries + 1} 次")
                if attempt < retries:
                    time.sleep(2 * (attempt + 1))
            except Exception as e:
                logger.warning(f"PanSou 搜索请求异常: {e}，第 {attempt + 1}/{retries + 1} 次")
                if attempt < retries:
                    time.sleep(2 * (attempt + 1))

        return None

    def _parse_results(self, results_list: List[Dict], keyword: str, limit: int) -> Dict[str, Any]:
        """解析旧版 data.results 列表格式"""
        grouped_results = {}

        for item in results_list:
            title = item.get("title", "")
            title = re.sub(r'<[^>]+>', '', title)

            if not self._title_matches_search_key(keyword, title):
                continue

            links = item.get("links", [])
            update_time = item.get("datetime", "")

            for link in links:
                pan_type = link.get("type", "unknown")
                type_display = self.TYPE_NAMES.get(pan_type, pan_type)

                if type_display not in grouped_results:
                    grouped_results[type_display] = []

                if len(grouped_results[type_display]) >= limit:
                    continue

                link_item = {
                    "url": link.get("url", ""),
                    "title": title,
                    "update_time": update_time
                }

                pwd = link.get("password", "")
                if pwd:
                    link_item["password"] = pwd

                grouped_results[type_display].append(link_item)

        return grouped_results

    def _parse_merged(self, merged_by_type: Dict[str, Any], keyword: str, limit: int) -> Dict[str, Any]:
        """
        解析新版 API 的 merged_by_type 格式

        merged_by_type 结构:
        {
          "115": [
            {
              "url": "https://115cdn.com/s/...?password=zc39",
              "password": "zc39",
              "note": "资源说明/标题",
              "datetime": "发布时间",
              "source": "tg:频道名"
            },
            ...
          ],
          ...
        }

        :param merged_by_type: 按网盘类型分组的搜索结果
        :param keyword: 搜索关键词（用于标题匹配过滤）
        :param limit: 每种类型的结果数限制
        :return: 按网盘类型中文名分组的结果
        """
        grouped_results = {}

        for pan_type, items in merged_by_type.items():
            type_display = self.TYPE_NAMES.get(pan_type, pan_type)

            if type_display not in grouped_results:
                grouped_results[type_display] = []

            for item in items or []:
                # merged 模式下标题在 note 字段
                title = item.get("note") or item.get("title") or ""
                title = re.sub(r'<[^>]+>', '', title)

                if not self._title_matches_search_key(keyword, title):
                    continue

                if len(grouped_results[type_display]) >= limit:
                    continue

                link_item = {
                    "url": item.get("url", ""),
                    "title": title,
                    "update_time": item.get("datetime", "")
                }

                pwd = item.get("password", item.get("pwd", ""))
                if pwd:
                    link_item["password"] = pwd

                grouped_results[type_display].append(link_item)

        return grouped_results

    def search_115(self, keyword: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        专门搜索 115 网盘资源

        :param keyword: 搜索关键词
        :param limit: 结果数量限制
        :return: 115 网盘资源列表
        """
        result = self.search(keyword=keyword, cloud_types=["115"], limit=limit)

        if result.get("error"):
            logger.error(f"搜索 115 资源失败: {result.get('error')}")
            return []

        return result.get("results", {}).get("115网盘", [])

    def get_api_call_count(self) -> int:
        """获取 API 调用次数"""
        return self._api_call_count

    def reset_api_call_count(self):
        """重置 API 调用计数器"""
        self._api_call_count = 0
