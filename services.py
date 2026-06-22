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


# ── Provider catalog ──────────────────────────────────────────────────────────
#
# Single source of truth for what the UI offers: the providers each builder above
# supports, the env var each one reads for its API key, default base URLs for the
# OpenAI-compatible LLM hosts, and curated (advisory) model / voice lists. The
# builders accept any model/voice string, so the frontend treats these lists as
# suggestions and still allows free-text entry.

# Deepgram Aura-2 voices (ids match the `aura-2-<name>-en` convention).
_DEEPGRAM_VOICES = [
    {"id": "aura-2-helena-en",   "name": "Helena",   "gender": "female", "accent": "American",  "description": "IVR, casual chat."},
    {"id": "aura-2-asteria-en",  "name": "Asteria",  "gender": "female", "accent": "American",  "description": "Natural, conversational."},
    {"id": "aura-2-hyperion-en", "name": "Hyperion", "gender": "male",   "accent": "Australian", "description": "Interview."},
    {"id": "aura-2-amalthea-en", "name": "Amalthea", "gender": "female", "accent": "Filipino",  "description": "Casual chat."},
    {"id": "aura-2-draco-en",    "name": "Draco",    "gender": "male",   "accent": "British",   "description": "Storytelling."},
    {"id": "aura-2-electra-en",  "name": "Electra",  "gender": "female", "accent": "American",  "description": "IVR, advertising, customer service."},
    {"id": "aura-2-pandora-en",  "name": "Pandora",  "gender": "female", "accent": "British",   "description": "IVR, informative."},
    {"id": "aura-2-zeus-en",     "name": "Zeus",     "gender": "male",   "accent": "American",  "description": "IVR."},
    {"id": "aura-2-athena-en",   "name": "Athena",   "gender": "female", "accent": "American",  "description": "Storytelling."},
]

_OPENAI_TTS_VOICES = [
    {"id": "alloy",   "name": "Alloy",   "gender": "neutral", "accent": "American", "description": "Balanced, neutral."},
    {"id": "echo",    "name": "Echo",    "gender": "male",    "accent": "American", "description": "Warm, measured."},
    {"id": "fable",   "name": "Fable",   "gender": "neutral", "accent": "British",  "description": "Expressive, storytelling."},
    {"id": "onyx",    "name": "Onyx",    "gender": "male",    "accent": "American", "description": "Deep, authoritative."},
    {"id": "nova",    "name": "Nova",    "gender": "female",  "accent": "American", "description": "Bright, friendly."},
    {"id": "shimmer", "name": "Shimmer", "gender": "female",  "accent": "American", "description": "Soft, gentle."},
]

_CARTESIA_VOICES = [
    {"id": "e07c00bc-4134-4eae-9ea4-1a55fb45746b", "name": "Default (Sonic)", "gender": "neutral", "accent": "American", "description": "Natural, low-latency."},
]

_ELEVENLABS_VOICES = [
    {"id": "EXAVITQu4vr4xnSDxMaL", "name": "Sarah",  "gender": "female", "accent": "American", "description": "Multilingual, natural."},
    {"id": "21m00Tcm4TlvDq8ikWAM", "name": "Rachel", "gender": "female", "accent": "American", "description": "Calm, multilingual."},
]

PROVIDER_CATALOG = {
    "stt": [
        {"id": "deepgram",   "label": "Deepgram",   "apiKeyEnv": "DEEPGRAM_API_KEY",
         "models": ["nova-3", "nova-2", "nova-2-general"]},
        {"id": "assemblyai", "label": "AssemblyAI", "apiKeyEnv": "ASSEMBLYAI_API_KEY", "models": []},
        {"id": "gladia",     "label": "Gladia",     "apiKeyEnv": "GLADIA_API_KEY",     "models": []},
        {"id": "openai",     "label": "OpenAI (Whisper)", "apiKeyEnv": "OPENAI_API_KEY",
         "models": ["whisper-1", "gpt-4o-transcribe", "gpt-4o-mini-transcribe"]},
        {"id": "groq",       "label": "Groq (Whisper)",   "apiKeyEnv": "GROQ_API_KEY",
         "models": ["whisper-large-v3", "whisper-large-v3-turbo"]},
    ],
    "llm": [
        {"id": "openai",     "label": "OpenAI",     "apiKeyEnv": "OPENAI_API_KEY",
         "models": ["gpt-4o", "gpt-4o-mini", "gpt-4.1", "gpt-4.1-mini"]},
        {"id": "anthropic",  "label": "Anthropic (Claude)", "apiKeyEnv": "ANTHROPIC_API_KEY",
         "models": ["claude-opus-4-8", "claude-sonnet-4-6", "claude-haiku-4-5-20251001"]},
        {"id": "google",     "label": "Google (Gemini)",    "apiKeyEnv": "GOOGLE_API_KEY",
         "models": ["gemini-2.0-flash", "gemini-2.0-pro", "gemini-1.5-pro", "gemini-1.5-flash"]},
        {"id": "groq",       "label": "Groq",       "apiKeyEnv": "GROQ_API_KEY", "baseUrl": _OPENAI_COMPAT_BASE_URLS["groq"],
         "models": ["llama-3.3-70b-versatile", "llama-3.1-8b-instant", "mixtral-8x7b-32768", "gemma2-9b-it"]},
        {"id": "openrouter", "label": "OpenRouter", "apiKeyEnv": "OPENROUTER_API_KEY", "baseUrl": _OPENAI_COMPAT_BASE_URLS["openrouter"], "models": []},
        {"id": "together",   "label": "Together",   "apiKeyEnv": "TOGETHER_API_KEY",   "baseUrl": _OPENAI_COMPAT_BASE_URLS["together"],   "models": []},
        {"id": "fireworks",  "label": "Fireworks",  "apiKeyEnv": "FIREWORKS_API_KEY",  "baseUrl": _OPENAI_COMPAT_BASE_URLS["fireworks"],  "models": []},
        {"id": "deepseek",   "label": "DeepSeek",   "apiKeyEnv": "DEEPSEEK_API_KEY",   "baseUrl": _OPENAI_COMPAT_BASE_URLS["deepseek"],
         "models": ["deepseek-chat", "deepseek-reasoner"]},
        {"id": "cerebras",   "label": "Cerebras",   "apiKeyEnv": "CEREBRAS_API_KEY",   "baseUrl": _OPENAI_COMPAT_BASE_URLS["cerebras"],   "models": []},
        {"id": "perplexity", "label": "Perplexity", "apiKeyEnv": "PERPLEXITY_API_KEY", "baseUrl": _OPENAI_COMPAT_BASE_URLS["perplexity"], "models": []},
        {"id": "ollama",     "label": "Ollama (local)", "apiKeyEnv": None, "baseUrl": _OPENAI_COMPAT_BASE_URLS["ollama"], "models": []},
    ],
    "tts": [
        {"id": "deepgram",   "label": "Deepgram (Aura-2)", "apiKeyEnv": "DEEPGRAM_API_KEY",
         "models": [], "voices": _DEEPGRAM_VOICES},
        {"id": "cartesia",   "label": "Cartesia",   "apiKeyEnv": "CARTESIA_API_KEY",
         "models": [], "voices": _CARTESIA_VOICES},
        {"id": "elevenlabs", "label": "ElevenLabs", "apiKeyEnv": "ELEVENLABS_API_KEY",
         "models": ["eleven_flash_v2_5", "eleven_multilingual_v2", "eleven_turbo_v2_5"], "voices": _ELEVENLABS_VOICES},
        {"id": "openai",     "label": "OpenAI",     "apiKeyEnv": "OPENAI_API_KEY",
         "models": ["tts-1", "tts-1-hd", "gpt-4o-mini-tts"], "voices": _OPENAI_TTS_VOICES},
    ],
}


# Exposed for manager-side validation / introspection — derived from the catalog
# above so the two can't drift. (Catalog ids must match the builder dict keys.)
KNOWN_PROVIDERS = {
    modality: sorted(p["id"] for p in providers)
    for modality, providers in PROVIDER_CATALOG.items()
}
