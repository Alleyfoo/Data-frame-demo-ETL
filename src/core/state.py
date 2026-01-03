"""Session state wrapper for Streamlit UI pages."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

import streamlit as st


class SessionState:
    """Wrapper around st.session_state with defaults and reset support."""

    def __init__(self, defaults: Mapping[str, Any] | None = None) -> None:
        self._defaults = dict(defaults or {})
        for key, value in self._defaults.items():
            st.session_state.setdefault(key, value)

    def __getattr__(self, name: str) -> Any:
        if name == "_defaults":
            return super().__getattribute__(name)
        if name in st.session_state:
            return st.session_state[name]
        if name in self._defaults:
            value = self._defaults[name]
            st.session_state[name] = value
            return value
        raise AttributeError(f"SessionState has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        if name == "_defaults":
            super().__setattr__(name, value)
            return
        st.session_state[name] = value

    def reset(self, keys: Iterable[str] | None = None) -> None:
        """Clear selected keys (or defaults) and restore defaults."""
        keys_to_clear = list(keys) if keys is not None else list(self._defaults.keys())
        for key in keys_to_clear:
            st.session_state.pop(key, None)
        for key in keys_to_clear:
            if key in self._defaults:
                st.session_state.setdefault(key, self._defaults[key])
