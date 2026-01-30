"""
搜索处理模块
负责所有搜索相关逻辑：PanSou
"""
from typing import Optional, List, Dict, Any

from app.log import logger
from app.schemas import MediaInfo
from app.schemas.types import MediaType


class SearchHandler:
    """搜索处理器"""

    def __init__(
        self,
        pansou_client,
        pansou_enabled: bool = False,
        only_115: bool = True,
        pansou_channels: str = ""
    ):
        """
        初始化搜索处理器

        :param pansou_client: PanSou 客户端实例
        :param pansou_enabled: 是否启用 PanSou
        :param only_115: 是否只搜索115网盘资源
        :param pansou_channels: PanSou 搜索频道
        """
        self._pansou_client = pansou_client
        self._pansou_enabled = pansou_enabled
        self._only_115 = only_115
        self._pansou_channels = pansou_channels

    def get_enabled_sources(self) -> List[str]:
        """
        获取已启用且可用的搜索源列表

        :return: 搜索源名称列表
        """
        sources = []

        # PanSou
        if self._pansou_enabled and self._pansou_client:
            sources.append("pansou")

        return sources

    def search_resources(
        self,
        mediainfo: MediaInfo,
        media_type: MediaType,
        season: Optional[int] = None
    ) -> List[Dict]:
        """
        统一的资源搜索方法，支持电影和电视剧

        注意：此方法主要供电影订阅使用。电视剧订阅使用 search_single_source 进行逐源搜索。

        :param mediainfo: 媒体信息
        :param media_type: 媒体类型（MOVIE 或 TV）
        :param season: 季号（电视剧必需）
        :return: 115网盘资源列表
        """
        sources = self.get_enabled_sources()

        for source in sources:
            results = self.search_single_source(source, mediainfo, media_type, season)
            if results:
                return results

        return []

    def search_single_source(
        self,
        source: str,
        mediainfo: MediaInfo,
        media_type: MediaType,
        season: Optional[int] = None
    ) -> List[Dict]:
        """
        使用指定的单一搜索源查询资源

        :param source: 搜索源名称 ("pansou")
        :param mediainfo: 媒体信息
        :param media_type: 媒体类型
        :param season: 季号（电视剧时使用）
        :return: 115网盘资源列表
        """
        if source == "pansou":
            if media_type == MediaType.MOVIE:
                search_keyword = f"{mediainfo.title} {mediainfo.year}" if mediainfo.year else mediainfo.title
                logger.info(f"使用 PanSou 搜索电影资源: {mediainfo.title}")
                return self._pansou_search(search_keyword)
            else:
                return self._search_pansou_tv(mediainfo, season)
        else:
            logger.warning(f"未知的搜索源: {source}")
            return []

    def _pansou_search(self, keyword: str) -> List[Dict]:
        """
        PanSou 搜索的通用逻辑

        :param keyword: 搜索关键词
        :return: 115网盘资源列表
        """
        cloud_types = ["115"] if self._only_115 else None

        channels = None
        if self._pansou_channels and self._pansou_channels.strip():
            channels = [ch.strip() for ch in self._pansou_channels.split(',') if ch.strip()]

        search_results = self._pansou_client.search(
            keyword=keyword,
            cloud_types=cloud_types,
            channels=channels,
            limit=20
        )

        results = search_results.get("results", {}) if search_results and not search_results.get("error") else {}
        return results.get("115网盘", [])

    def _search_pansou_tv(
        self,
        mediainfo: MediaInfo,
        season: int
    ) -> List[Dict]:
        """
        仅使用 PanSou 搜索电视剧资源（带降级关键词策略）

        :param mediainfo: 媒体信息
        :param season: 季号
        :return: 115网盘资源列表
        """
        if not self._pansou_client:
            logger.warning(f"PanSou 客户端未初始化，跳过 PanSou 查询")
            return []

        # 电视剧使用降级搜索策略
        search_keywords = [
            f"{mediainfo.title}{season}",  # 中文季号格式
            mediainfo.title
        ]

        for keyword in search_keywords:
            logger.info(f"使用 PanSou 搜索电视剧资源: {mediainfo.title} S{season}，关键词: '{keyword}'")
            results = self._pansou_search(keyword)
            if results:
                logger.info(f"PanSou 关键词 '{keyword}' 搜索到 {len(results)} 个结果")
                return results
            else:
                logger.info(f"PanSou 关键词 '{keyword}' 无结果，尝试下一个降级关键词")

        logger.info(f"PanSou 未找到资源")
        return []
