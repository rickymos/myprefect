"""Local Python startup customizations for the Prefect orchestration project.

This suppresses a known upstream Prefect/Pydantic warning about the default
timezone field (`UTC`) so service logs stay readable.
"""

from __future__ import annotations

import warnings

try:
    from pydantic.warnings import UnsupportedFieldAttributeWarning
except Exception:  # pragma: no cover - defensive import for startup path safety
    UnsupportedFieldAttributeWarning = Warning


warnings.filterwarnings(
    "ignore",
    message=r"The 'default' attribute with value 'UTC' was provided to the `Field\(\)` function.*",
    category=UnsupportedFieldAttributeWarning,
)

warnings.filterwarnings(
    "ignore",
    message=r"Config key `pyproject_toml_table_header` is set in model_config but will be ignored.*",
    category=UserWarning,
    module=r"pydantic_settings\.main",
)

warnings.filterwarnings(
    "ignore",
    message=r"Config key `toml_file` is set in model_config but will be ignored.*",
    category=UserWarning,
    module=r"pydantic_settings\.main",
)