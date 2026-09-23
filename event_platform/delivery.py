from __future__ import annotations

import math
from dataclasses import dataclass
from enum import StrEnum
from numbers import Real


class DeliveryDisposition(StrEnum):
    RETRYABLE = "retryable"
    PERMANENT = "permanent"


class DeliveryError(RuntimeError):
    """Base class for transport adapters that can classify publish failures."""

    disposition: DeliveryDisposition
    code: str

    def __init__(
        self,
        message: str,
        *,
        retry_after_seconds: float | None = None,
    ) -> None:
        if not isinstance(message, str) or not message.strip():
            raise ValueError("delivery error message must be non-empty")
        if retry_after_seconds is not None and (
            not isinstance(retry_after_seconds, Real)
            or isinstance(retry_after_seconds, bool)
            or not math.isfinite(float(retry_after_seconds))
            or retry_after_seconds < 0
        ):
            raise ValueError("retry_after_seconds must be finite and non-negative")
        if self.disposition is DeliveryDisposition.PERMANENT and retry_after_seconds is not None:
            raise ValueError("permanent delivery errors cannot carry a retry hint")
        super().__init__(message)
        self.retry_after_seconds = (
            None if retry_after_seconds is None else float(retry_after_seconds)
        )


class RetryableDeliveryError(DeliveryError):
    """A transient transport failure that may include a bounded server retry hint."""

    disposition = DeliveryDisposition.RETRYABLE
    code = "RETRYABLE_DELIVERY"


class PermanentDeliveryError(DeliveryError):
    """A protocol or policy failure that must not consume the retry budget."""

    disposition = DeliveryDisposition.PERMANENT
    code = "PERMANENT_DELIVERY"


@dataclass(frozen=True, slots=True)
class ClassifiedFailure:
    disposition: DeliveryDisposition
    code: str
    message: str
    retry_after_seconds: float | None

    @property
    def reason(self) -> str:
        return f"{self.code}: {self.message}"


def classify_delivery_failure(error: Exception) -> ClassifiedFailure:
    """Normalize adapter and unknown failures into bounded persistence evidence."""

    if isinstance(error, DeliveryError):
        disposition = error.disposition
        code = error.code
        retry_after = error.retry_after_seconds
    else:
        # Existing publishers may raise connection-library exceptions directly. Unknown
        # failures remain retryable for compatibility, but carry an explicit evidence code.
        disposition = DeliveryDisposition.RETRYABLE
        code = "UNCLASSIFIED_DELIVERY"
        retry_after = None

    message = " ".join(str(error).split()) or type(error).__name__
    return ClassifiedFailure(disposition, code, message[:900], retry_after)
