from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class InputConfig:
    name: str
    extractor: str
    uri: str
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class OutputConfig:
    name: str
    loader: str
    input: str
    uri: str
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class TransformConfig:
    name: str
    transformer: str
    inputs: Dict[str, str]
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class ValidationConfig:
    name: str
    validator: str
    input: str
    parameters: Optional[Dict[str, Any]] = None


@dataclass
class Config:
    inputs: List[InputConfig]
    transforms: Optional[List[TransformConfig]]
    validations: Optional[List[ValidationConfig]]
    outputs: List[OutputConfig]
