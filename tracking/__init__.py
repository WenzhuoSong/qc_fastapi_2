# tracking/
"""
W&B experiment tracking utilities.

PipelineRunTracker: logs pipeline run metadata, stage metrics, and final
decisions to Weights & Biases. Safe to use — all methods are no-ops if
WANDB_API_KEY is not configured or unavailable.
"""
from .wandb_client import PipelineRunTracker

__all__ = [
    "PipelineRunTracker",
]
