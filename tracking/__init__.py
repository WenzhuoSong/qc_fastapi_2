# tracking/
"""
W&B experiment tracking + DVC data export utilities.

PipelineRunTracker: logs pipeline run metadata, stage metrics, and final
decisions to Weights & Biases. Safe to use — all methods are no-ops if
WANDB_API_KEY is not configured or unavailable.

DVC exporter functions: export pipeline results and news snapshots to
parquet files for DVC versioning.
"""
from .wandb_client import PipelineRunTracker
from .dvc_exporter import export_pipeline_results, export_strategy_params, export_news_snapshot

__all__ = [
    "PipelineRunTracker",
    "export_pipeline_results",
    "export_strategy_params",
    "export_news_snapshot",
]
