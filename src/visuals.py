"""Seaborn plotting helpers: data in, figure out."""
from __future__ import annotations

import matplotlib.pyplot as plt  # seaborn's figure backend
import pandas as pd
import seaborn as sns

sns.set_theme(style="whitegrid", palette="colorblind")
PRIMARY = sns.color_palette("colorblind")[0]


def plot_model_comparison(metrics: pd.DataFrame, metric: str = "accuracy"):
    """Bar chart ranking models by one validation metric."""
    ranked = metrics.sort_values(metric, ascending=False)
    fig, ax = plt.subplots(figsize=(6, 4))
    sns.barplot(data=ranked, x="model", y=metric, color=PRIMARY, ax=ax)
    ax.set_ylim(0, 1)
    ax.set_xlabel("")
    ax.set_title(f"Validation {metric} by model")
    for container in ax.containers:
        ax.bar_label(container, fmt="%.3f", padding=2)
    sns.despine(ax=ax)
    fig.tight_layout()
    return fig


def plot_metric_comparison(metrics: pd.DataFrame, metric_cols: list[str]):
    """Grouped bars comparing several metrics across models."""
    long = metrics.melt(id_vars="model", value_vars=metric_cols,
                        var_name="metric", value_name="score")
    fig, ax = plt.subplots(figsize=(7, 4))
    sns.barplot(data=long, x="metric", y="score", hue="model", ax=ax)
    ax.set_ylim(0, 1)
    ax.set_xlabel("")
    ax.set_title("Validation metrics by model")
    sns.despine(ax=ax)
    fig.tight_layout()
    return fig


def plot_confusion_matrix(matrix, labels: list[str], model_name: str = ""):
    """Heatmap of a confusion matrix (rows actual, cols predicted)."""
    fig, ax = plt.subplots(figsize=(4.5, 4))
    sns.heatmap(matrix, annot=True, fmt="d", cmap="Blues", cbar=False,
                square=True, xticklabels=labels, yticklabels=labels, ax=ax)
    ax.set_xlabel("Predicted")
    ax.set_ylabel("Actual")
    ax.set_title(f"Confusion matrix {model_name}".strip())
    fig.tight_layout()
    return fig
