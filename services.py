"""Config-driven STT / LLM / TTS service factories for the voice bot.

Each agent is launched as a ``bot.py`` subprocess with its ``config`` dict merged
into the environment (see ``manager.py``). This module turns a few env keys —
``STT_PROVIDER`` / ``LLM_PROVIDER`` / ``TTS_PROVIDER`` plus provider-specific
keys, models and voices — into the actual Pipecat service objects.

Design notes:

- Pipecat service imports are **lazy** (done inside each builder) so a missing
  optional extra only affects the provider that needs it, not the whole process.
- Each builder raises on a missing / placeholder key. The dispatcher catches
  that and falls back to the proven default for that modality (Deepgram STT,
  OpenAI-compatible LLM, Deepgram TTS) — mirroring the original
  ``_build_tts_service`` behaviour so existing agents keep working unchanged.
"""

import os

from loguru import logger

DEFAULT_STT = "deepgram"
DEFAULT_LLM = "openai"
DEFAULT_TTS = "deepgram"


def _has_real_value(value: str | None) -> bool:
    """True unless the value is empty or a leftover ``your_...`` placeholder."""
    return bool(value and value.strip() and not value.strip().lower().startswith("your_"))


def _get(*names: str, default: str | None = None) -> str | None:
    """Return the first non-empty env var among ``names`` (else ``default``)."""
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


# ── STT builders ────────────────────────────────────────────────────────────

def _build_deepgram_stt():
    from pipecat.services.deepgram.stt import DeepgramSTTService

    key = os.getenv("DEEPGRAM_API_KEY")
    if not _has_real_value(key):
        raise ValueError("DEEPGRAM_API_KEY is missing or a placeholder")
    return DeepgramSTTService(api_key=key)


def _build_assemblyai_stt():
    from pipecat.services.assemblyai.stt import AssemblyAISTTService

    key = os.getenv("ASSEMBLYAI_API_KEY")
    if not _has_real_value(key):
        raise ValueError("ASSEMBLYAI_API_KEY is missing or a placeholder")
    return AssemblyAISTTService(api_key=key)


def _build_gladia_stt():
    from pipecat.services.gladia.stt import GladiaSTTService

    key = os.getenv("GLADIA_API_KEY")
    if not _has_real_value(key):
        raise ValueError("GLADIA_API_KEY is missing or a placeholder")
    return GladiaSTTService(api_key=key)


def _build_openai_stt():
    from pipecat.services.openai.stt import OpenAISTTService

    key = os.getenv("OPENAI_API_KEY")
    if not _has_real_value(key):
        raise ValueError("OPENAI_API_KEY is missing or a placeholder")
    kwargs = {"api_key": key}
    model = _get("STT_MODEL")
    if model:
        kwargs["model"] = model
    return OpenAISTTService(**kwargs)


def _build_groq_stt():
    from pipecat.services.groq.stt import GroqSTTService

    key = _get("GROQ_API_KEY", "OPENAI_API_KEY")
    if not _has_real_value(key):
        raise ValueError("GROQ_API_KEY (or OPENAI_API_KEY) is missing or a placeholder")
    kwargs = {"api_key": key}
    model = _get("STT_MODEL")
    if model:
        kwargs["model"] = model
    return GroqSTTService(**kwargs)


_STT_BUILDERS = {
    "deepgram": _build_deepgram_stt,
    "assemblyai": _build_assemblyai_stt,
    "gladia": _build_gladia_stt,
    "openai": _build_openai_stt,
    "groq": _build_groq_stt,
}


# ── LLM builders ────────────────────────────────────────────────────────────

def _build_openai_llm():
    """OpenAI and any OpenAI-compatible endpoint (set ``OPENAI_BASE_URL``)."""
    from pipecat.services.openai.llm import OpenAILLMService

    key = os.getenv("OPENAI_API_KEY")
    if not _has_real_value(key):
        raise ValueError("OPENAI_API_KEY is missing or a placeholder")
    kwargs = {"api_key": key}
    base_url = _get("LLM_BASE_URL", "OPENAI_BASE_URL")
    if base_url:
        kwargs["base_url"] = base_url
    model = _get("LLM_MODEL", "OPENAI_MODEL")
    if model:
        kwargs["model"] = model
    return OpenAILLMService(**kwargs)


def _build_anthropic_llm():
    from pipecat.services.anthropic.llm import AnthropicLLMService

    key = os.getenv("ANTHROPIC_API_KEY")
    if not _has_real_value(key):
        raise ValueError("ANTHROPIC_API_KEY is missing or a placeholder")
    kwargs = {"api_key": key}
    model = _get("LLM_MODEL", "ANTHROPIC_MODEL")
    if model:
        kwargs["model"] = model
    return AnthropicLLMService(**kwargs)


def _build_google_llm():
    from pipecat.services.google.llm import GoogleLLMService

    key = _get("GOOGLE_API_KEY", "GEMINI_API_KEY")
    if not _has_real_value(key):
        raise ValueError("GOOGLE_API_KEY (or GEMINI_API_KEY) is missing or a placeholder")
    kwargs = {"api_key": key}
    model = _get("LLM_MODEL", "GOOGLE_MODEL")
    if model:
        kwargs["model"] = model
    return GoogleLLMService(**kwargs)


# OpenAI-compatible hosts: same OpenAILLMService class, just a preset base_url.
# Per-agent override still honoured via LLM_BASE_URL.
_OPENAI_COMPAT_BASE_URLS = {
    "groq": "https://api.groq.com/openai/v1",
    "openrouter": "https://openrouter.ai/api/v1",
    "together": "https://api.together.xyz/v1",
    "fireworks": "https://api.fireworks.ai/inference/v1",
    "deepseek": "https://api.deepseek.com/v1",
    "cerebras": "https://api.cerebras.ai/v1",
    "perplexity": "https://api.perplexity.ai",
    "ollama": "http://localhost:11434/v1",
}


def _make_openai_compat_llm_builder(name: str, default_base_url: str):
    def _builder():
        from pipecat.services.openai.llm import OpenAILLMService

        key = _get(f"{name.upper()}_API_KEY", "OPENAI_API_KEY", "LLM_API_KEY")
        # Local runtimes (Ollama / vLLM) don't require a real key.
        if name != "ollama" and not _has_real_value(key):
            raise ValueError(f"{name.upper()}_API_KEY (or OPENAI_API_KEY) is missing or a placeholder")
        kwargs = {
            "api_key": key or "local",
            "base_url": _get("LLM_BASE_URL", default=default_base_url),
        }
        model = _get("LLM_MODEL")
        if model:
            kwargs["model"] = model
        return OpenAILLMService(**kwargs)

    return _builder


_LLM_BUILDERS = {
    "openai": _build_openai_llm,
    "anthropic": _build_anthropic_llm,
    "google": _build_google_llm,
    **{
        name: _make_openai_compat_llm_builder(name, base_url)
        for name, base_url in _OPENAI_COMPAT_BASE_URLS.items()
    },
}


# ── TTS builders ────────────────────────────────────────────────────────────

def _build_deepgram_tts():
    from pipecat.services.deepgram.tts import DeepgramTTSService

    key = os.getenv("DEEPGRAM_API_KEY")
    if not _has_real_value(key):
        raise ValueError("DEEPGRAM_API_KEY is missing or a placeholder")
    voice = _get("TTS_VOICE", "DEEPGRAM_TTS_VOICE", default="aura-2-helena-en")
    return DeepgramTTSService(
        api_key=key,
        settings=DeepgramTTSService.Settings(voice=voice),
    )


def _build_cartesia_tts():
    from pipecat.services.cartesia.tts import CartesiaTTSService

    key = os.getenv("CARTESIA_API_KEY")
    if not _has_real_value(key):
        raise ValueError("CARTESIA_API_KEY is missing or a placeholder")
    voice = _get(
        "TTS_VOICE", "CARTESIA_TTS_VOICE", default="e07c00bc-4134-4eae-9ea4-1a55fb45746b"
    )
    return CartesiaTTSService(
        api_key=key,
        settings=CartesiaTTSService.Settings(voice=voice),
    )


def _build_elevenlabs_tts():
    from pipecat.services.elevenlabs.tts import ElevenLabsTTSService

    key = os.getenv("ELEVENLABS_API_KEY")
    if not _has_real_value(key):
        raise ValueError("ELEVENLABS_API_KEY is missing or a placeholder")
    kwargs = {
        "api_key": key,
        "voice_id": _get("TTS_VOICE", "ELEVENLABS_VOICE_ID", default="EXAVITQu4vr4xnSDxMaL"),
    }
    model = _get("TTS_MODEL")
    if model:
        kwargs["model"] = model
    return ElevenLabsTTSService(**kwargs)


def _build_openai_tts():
    from pipecat.services.openai.tts import OpenAITTSService

    key = os.getenv("OPENAI_API_KEY")
    if not _has_real_value(key):
        raise ValueError("OPENAI_API_KEY is missing or a placeholder")
    kwargs = {"api_key": key, "voice": _get("TTS_VOICE", default="alloy")}
    model = _get("TTS_MODEL")
    if model:
        kwargs["model"] = model
    return OpenAITTSService(**kwargs)


_TTS_BUILDERS = {
    "deepgram": _build_deepgram_tts,
    "cartesia": _build_cartesia_tts,
    "elevenlabs": _build_elevenlabs_tts,
    "openai": _build_openai_tts,
}


# ── Dispatcher ──────────────────────────────────────────────────────────────

def _build_with_fallback(modality: str, provider: str, builders: dict, default: str):
    builder = builders.get(provider)
    if builder is None:
        logger.warning(
            f"{modality}: unknown provider '{provider}' "
            f"(known: {', '.join(sorted(builders))}); falling back to '{default}'"
        )
        provider, builder = default, builders[default]

    try:
        service = builder()
        logger.info(f"{modality} provider: {provider} -> {type(service).__name__}")
        return service
    except Exception as exc:
        if provider == default:
            # Default itself failed — nothing left to fall back to.
            raise
        logger.warning(
            f"{modality}: provider '{provider}' failed ({exc}); falling back to '{default}'"
        )
        service = builders[default]()
        logger.info(f"{modality} provider: {default} (fallback) -> {type(service).__name__}")
        return service


def build_stt_service():
    provider = (os.getenv("STT_PROVIDER", DEFAULT_STT) or DEFAULT_STT).strip().lower()
    return _build_with_fallback("STT", provider, _STT_BUILDERS, DEFAULT_STT)


def build_llm_service():
    provider = (os.getenv("LLM_PROVIDER", DEFAULT_LLM) or DEFAULT_LLM).strip().lower()
    return _build_with_fallback("LLM", provider, _LLM_BUILDERS, DEFAULT_LLM)


def build_tts_service():
    provider = (os.getenv("TTS_PROVIDER", DEFAULT_TTS) or DEFAULT_TTS).strip().lower()
    return _build_with_fallback("TTS", provider, _TTS_BUILDERS, DEFAULT_TTS)


# Exposed for manager-side validation / introspection.
KNOWN_PROVIDERS = {
    "stt": sorted(_STT_BUILDERS),
    "llm": sorted(_LLM_BUILDERS),
    "tts": sorted(_TTS_BUILDERS),
}
