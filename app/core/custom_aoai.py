"""
AzureChatOpenAI Wrapper with Dynamic Header Generation Callback

이 wrapper는 AzureChatOpenAI를 확장하여:
1. 초기화 시 header_generation_callback을 등록
2. invoke/ainvoke/stream/astream 호출 시마다 callback을 실행하여 동적으로 헤더 생성
3. 생성된 헤더를 기존 헤더에 merge하여 요청 전송

langchain-core 0.3.48, langchain 0.3.21, langchain-openai 호환

사용 예시:
    ```python
    import random

    def random_user_header():
        return {"x-client-user": f"user-{random.randint(1000, 9999)}"}

    llm = AzureChatOpenAIWithDynamicHeaders(
        azure_deployment="gpt-4",
        header_generation_callback=random_user_header,
    )

    # 매 호출마다 callback이 실행되어 x-client-user가 랜덤하게 설정됨
    response = llm.invoke("Hello!")  # x-client-user: user-1234
    response = llm.invoke("World!")  # x-client-user: user-5678 (다른 값)
    ```
"""

from __future__ import annotations

from typing import Any, AsyncIterator, Callable, Iterator, List, Mapping, Optional

import openai
from langchain_core.callbacks import (
    AsyncCallbackManagerForLLMRun,
    CallbackManagerForLLMRun,
)
from langchain_core.messages import BaseMessage
from langchain_core.outputs import ChatGenerationChunk, ChatResult
from langchain_openai import AzureChatOpenAI
from pydantic import Field


# Type alias for header generation callback
HeaderGenerationCallback = Callable[[], Mapping[str, str]]


class AzureChatOpenAIWithDynamicHeaders(AzureChatOpenAI):
    """
    AzureChatOpenAI wrapper that supports dynamic header generation via callback.

    매 API 호출마다 header_generation_callback이 실행되어 동적으로 헤더를 생성합니다.
    이는 상위 프레임워크(LangGraph 등)가 LLM을 wrapping할 때 호출 시점에
    extra_headers를 전달할 수 없는 문제를 해결합니다.

    Usage:
        ```python
        import random

        def random_user_header():
            return {"x-client-user": f"user-{random.randint(1000, 9999)}"}

        llm = AzureChatOpenAIWithDynamicHeaders(
            azure_deployment="gpt-4",
            azure_endpoint="https://xxx.openai.azure.com/",
            api_version="2024-02-15-preview",
            header_generation_callback=random_user_header,
        )

        # 매 호출마다 callback이 실행됨
        response = llm.invoke("Hello!")
        response = await llm.ainvoke("Hello!")
        ```
    """

    header_generation_callback: Optional[HeaderGenerationCallback] = Field(
        default=None,
        exclude=True,  # serialization에서 제외
    )
    """Callback function that generates headers for each request."""

    class Config:
        """Pydantic config."""
        arbitrary_types_allowed = True

    def __init__(
        self,
        header_generation_callback: Optional[HeaderGenerationCallback] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize with header generation callback.

        Args:
            header_generation_callback: A callable that returns a dict of headers.
                                        Called on every API request.
            **kwargs: All other AzureChatOpenAI parameters.
        """
        super().__init__(**kwargs)
        self.header_generation_callback = header_generation_callback

    def _get_dynamic_headers(self) -> Mapping[str, str]:
        """
        Call the header generation callback to get dynamic headers.
        Returns empty dict if no callback is set.
        """
        if self.header_generation_callback is None:
            return {}
        return self.header_generation_callback()

    def _create_client_with_headers(
        self,
        extra_headers: Mapping[str, str],
        is_async: bool = False,
    ) -> Any:
        """
        Create a new OpenAI client with merged headers.

        Args:
            extra_headers: Additional headers to merge with default_headers.
            is_async: Whether to create async or sync client.

        Returns:
            chat.completions client (sync or async)
        """
        # 기존 default_headers와 동적 헤더 병합
        base_headers = dict(self.default_headers or {})
        merged_headers = {**base_headers, **extra_headers}

        client_params = {
            "api_version": self.openai_api_version,
            "azure_endpoint": self.azure_endpoint,
            "azure_deployment": self.deployment_name,
            "api_key": (
                self.openai_api_key.get_secret_value()
                if self.openai_api_key
                else None
            ),
            "azure_ad_token": (
                self.azure_ad_token.get_secret_value()
                if self.azure_ad_token
                else None
            ),
            "azure_ad_token_provider": self.azure_ad_token_provider,
            "organization": self.openai_organization,
            "base_url": self.openai_api_base,
            "timeout": self.request_timeout,
            "default_headers": merged_headers,
            "default_query": self.default_query,
        }

        if self.max_retries is not None:
            client_params["max_retries"] = self.max_retries

        if is_async:
            if self.http_async_client:
                client_params["http_client"] = self.http_async_client
            if self.azure_ad_async_token_provider:
                client_params["azure_ad_token_provider"] = (
                    self.azure_ad_async_token_provider
                )
            return openai.AsyncAzureOpenAI(**client_params).chat.completions
        else:
            if self.http_client:
                client_params["http_client"] = self.http_client
            return openai.AzureOpenAI(**client_params).chat.completions

    # =========================================================================
    # Override _generate for sync operations
    # =========================================================================

    def _generate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        """
        Override _generate to inject dynamic headers on each call.
        """
        dynamic_headers = self._get_dynamic_headers()

        if dynamic_headers:
            # 임시로 클라이언트 교체
            original_client = self.client
            try:
                self.client = self._create_client_with_headers(
                    dynamic_headers, is_async=False
                )
                return super()._generate(messages, stop, run_manager, **kwargs)
            finally:
                self.client = original_client
        else:
            return super()._generate(messages, stop, run_manager, **kwargs)

    # =========================================================================
    # Override _agenerate for async operations
    # =========================================================================

    async def _agenerate(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> ChatResult:
        """
        Override _agenerate to inject dynamic headers on each call.
        """
        dynamic_headers = self._get_dynamic_headers()

        if dynamic_headers:
            original_client = self.async_client
            try:
                self.async_client = self._create_client_with_headers(
                    dynamic_headers, is_async=True
                )
                return await super()._agenerate(messages, stop, run_manager, **kwargs)
            finally:
                self.async_client = original_client
        else:
            return await super()._agenerate(messages, stop, run_manager, **kwargs)

    # =========================================================================
    # Override _stream for sync streaming
    # =========================================================================

    def _stream(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> Iterator[ChatGenerationChunk]:
        """
        Override _stream to inject dynamic headers on each call.
        """
        dynamic_headers = self._get_dynamic_headers()

        if dynamic_headers:
            original_client = self.client
            try:
                self.client = self._create_client_with_headers(
                    dynamic_headers, is_async=False
                )
                yield from super()._stream(messages, stop, run_manager, **kwargs)
            finally:
                self.client = original_client
        else:
            yield from super()._stream(messages, stop, run_manager, **kwargs)

    # =========================================================================
    # Override _astream for async streaming
    # =========================================================================

    async def _astream(
        self,
        messages: List[BaseMessage],
        stop: Optional[List[str]] = None,
        run_manager: Optional[AsyncCallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        """
        Override _astream to inject dynamic headers on each call.
        """
        dynamic_headers = self._get_dynamic_headers()

        if dynamic_headers:
            original_client = self.async_client
            try:
                self.async_client = self._create_client_with_headers(
                    dynamic_headers, is_async=True
                )
                async for chunk in super()._astream(
                    messages, stop, run_manager, **kwargs
                ):
                    yield chunk
            finally:
                self.async_client = original_client
        else:
            async for chunk in super()._astream(
                messages, stop, run_manager, **kwargs
            ):
                yield chunk
