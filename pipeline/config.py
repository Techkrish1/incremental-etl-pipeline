from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, field_validator


class QualityRuleConfig(BaseModel):
    max_null_rate: float = Field(default=0.05, ge=0.0, le=1.0)
    require_unique_key: bool = True
    max_duplicate_rate: float = Field(default=0.0, ge=0.0, le=1.0)
    freshness_sla_hours: int | None = None


class SchemaFieldConfig(BaseModel):
    name: str
    dtype: Literal["string", "integer", "float", "boolean", "timestamp"]
    nullable: bool = True
    primary_key: bool = False


class PipelineConfig(BaseModel):
    name: str
    source_dir: Path
    warehouse_path: Path
    checkpoint_path: Path
    target_table: str
    merge_key: str
    updated_at_column: str = "updated_at"
    batch_size: int = Field(default=10_000, gt=0)
    schema_fields: list[SchemaFieldConfig] = Field(default_factory=list)
    quality: QualityRuleConfig = Field(default_factory=QualityRuleConfig)
    dead_letter_dir: Path | None = None
    file_pattern: str = "*.csv"

    @field_validator("source_dir", "warehouse_path", "checkpoint_path", mode="before")
    @classmethod
    def expand_path(cls, v: str | Path) -> Path:
        return Path(v).expanduser()

    @property
    def primary_key_fields(self) -> list[str]:
        pks = [f.name for f in self.schema_fields if f.primary_key]
        return pks if pks else [self.merge_key]

    @classmethod
    def from_yaml(cls, path: str | Path) -> "PipelineConfig":
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls(**data)
