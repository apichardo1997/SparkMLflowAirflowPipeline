"""B.1/B.2: spark.ml classification + MLflow tracking, registry, and deploy-best.

train() builds a pipeline per classifier, ranks by validation accuracy, logs every
run to MLflow with the data fingerprint as version, registers + promotes the best
model, then reloads it from the registry to prove reproducibility. Feature/target
columns and model name come from config (no hardcoding)."""
from __future__ import annotations

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import DataFrame, SparkSession

from src.checkpoints import data_fingerprint
from src.utils.config import load_config, zone_path

# Logged for every model; first entry is the ranking/deploy metric.
METRICS = ["accuracy", "weightedRecall", "weightedPrecision", "f1"]

# Shared seed for reproducible splits and tuning.
SEED = 42

LABEL_COL = "label"
FEATURES_COL = "features"


def evaluate(predictions, label_col: str = "label", pred_col: str = "prediction") -> dict:
    """Validation metrics: accuracy, weighted recall, weighted precision, F1."""
    return {
        metric: MulticlassClassificationEvaluator(
            labelCol=label_col, predictionCol=pred_col, metricName=metric
        ).evaluate(predictions)
        for metric in METRICS
    }


def per_class_metrics(predictions, label_order: list[str], label_col: str = "label",
                      pred_col: str = "prediction") -> dict:
    """Per-class recall/precision keyed by original tier name (recall_low, precision_high)."""
    out = {}
    for idx, name in enumerate(label_order):
        for metric, prefix in (("recallByLabel", "recall"), ("precisionByLabel", "precision")):
            out[f"{prefix}_{name}"] = MulticlassClassificationEvaluator(
                labelCol=label_col, predictionCol=pred_col,
                metricName=metric, metricLabel=float(idx),
            ).evaluate(predictions)
    return out


def confusion_matrix(predictions, label_order: list[str], label_col: str = "label",
                     pred_col: str = "prediction") -> list[list[int]]:
    """Counts as rows=actual, cols=predicted, ordered by label_order."""
    n = len(label_order)
    counts = {(int(r[label_col]), int(r[pred_col])): r["n"]
              for r in predictions.groupBy(label_col, pred_col).count()
              .withColumnRenamed("count", "n").collect()}
    return [[counts.get((i, j), 0) for j in range(n)] for i in range(n)]


def label_order(fitted_pipeline) -> list[str]:
    """Tier names indexed by label value, read from the fitted target StringIndexer."""
    return list(fitted_pipeline.stages[0].labels)


def cross_validate(estimator, param_grid, train_df, num_folds: int = 5,
                   seed: int = SEED, label_col: str = "label",
                   pred_col: str = "prediction", metric: str = "accuracy"):
    """K-fold grid search; returns the fitted CrossValidatorModel (best = .bestModel)."""
    evaluator = MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol=pred_col, metricName=metric
    )
    cv = CrossValidator(
        estimator=estimator,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=num_folds,
        seed=seed,
    )
    return cv.fit(train_df)


def load_training_df(spark: SparkSession, config: dict) -> DataFrame:
    """Read the exploitation table (Part A output) named in config.analysis.table."""
    table = config["analysis"]["table"]
    path = zone_path(config, "exploitation") / table
    fmt = "delta" if config.get("output_format", "parquet").lower() == "delta" else "parquet"
    return spark.read.format(fmt).load(str(path))


def build_pipeline(numeric_features: list[str], categorical_features: list[str],
                   classifier, target_col: str) -> Pipeline:
    """label indexer -> per-categorical index+OHE -> assembler -> classifier."""
    stages = [StringIndexer(inputCol=target_col, outputCol=LABEL_COL,
                            handleInvalid="keep")]
    assembler_inputs = list(numeric_features)
    for col in categorical_features:
        stages.append(StringIndexer(inputCol=col, outputCol=f"{col}_idx",
                                    handleInvalid="keep"))
        stages.append(OneHotEncoder(inputCol=f"{col}_idx", outputCol=f"{col}_ohe"))
        assembler_inputs.append(f"{col}_ohe")
    stages.append(VectorAssembler(inputCols=assembler_inputs, outputCol=FEATURES_COL))
    stages.append(classifier)
    return Pipeline(stages=stages)


def model_specs(seed: int = SEED) -> dict:
    """Two distinct algorithms, each with a hyperparameter grid to tune (spec: two
    different algorithms, each searched over its own configurations for the best)."""
    lr = LogisticRegression(featuresCol=FEATURES_COL, labelCol=LABEL_COL, maxIter=50)
    rf = RandomForestClassifier(featuresCol=FEATURES_COL, labelCol=LABEL_COL, seed=seed)
    return {
        "logistic_regression": {
            "clf": lr,
            "grid": ParamGridBuilder()
            .addGrid(lr.regParam, [0.01, 0.1, 0.5])
            .addGrid(lr.elasticNetParam, [0.0, 0.5])
            .build(),
        },
        "random_forest": {
            "clf": rf,
            "grid": ParamGridBuilder()
            .addGrid(rf.numTrees, [50, 100])
            .addGrid(rf.maxDepth, [5, 10])
            .build(),
        },
    }


def _hyperparams(classifier) -> dict:
    """Hyperparameters set on a classifier, for MLflow params."""
    return {p.name: classifier.getOrDefault(p)
            for p in classifier.params if classifier.isSet(p)}


def promote_best(client, name: str, version: str, stage: str = "Production") -> str:
    """Move a model version to a stage. Falls back to aliases on MLflow>=3 (stages dropped)."""
    try:
        client.transition_model_version_stage(
            name=name, version=version, stage=stage,
            archive_existing_versions=True,
        )
        return f"models:/{name}/{stage}"
    except Exception:
        alias = stage.lower()
        client.set_registered_model_alias(name=name, alias=alias, version=version)
        return f"models:/{name}@{alias}"


def train(spark: SparkSession, config: dict, df: DataFrame | None = None,
          specs: dict | None = None, num_folds: int = 3, seed: int = SEED) -> dict:
    """Tune each algorithm over its grid (CV), rank best configs by validation accuracy,
    register + promote the winner, reload from the registry and re-score. Returns a summary."""
    import mlflow
    import mlflow.spark

    analysis = config["analysis"]
    target = analysis["target"]
    numeric = list(analysis["feature_columns"])
    categorical = list(analysis.get("categorical_columns", []))
    model_name = analysis["model_name"]
    split = analysis.get("train_fraction", 0.8)

    if df is None:
        df = load_training_df(spark, config)
    fingerprint = data_fingerprint(df)
    # Stable sort so the seeded split is identical here and in the B.3 results cell;
    # sort on all columns to not depend on which id columns Part A kept.
    df = df.orderBy(*df.columns)
    train_df, val_df = df.randomSplit([split, 1 - split], seed=seed)
    train_df.cache()

    specs = specs or model_specs(seed)
    client = mlflow.tracking.MlflowClient()

    results = []
    for name, spec in specs.items():
        with mlflow.start_run(run_name=name) as run:
            pipeline = build_pipeline(numeric, categorical, spec["clf"], target)
            model = cross_validate(pipeline, spec["grid"], train_df,
                                   num_folds=num_folds, seed=seed).bestModel
            preds = model.transform(val_df)
            order = label_order(model)
            metrics = evaluate(preds)
            mlflow.set_tag("data_fingerprint", fingerprint)
            mlflow.set_tag("label_order", ",".join(order))
            mlflow.log_params({
                "algorithm": spec["clf"].__class__.__name__,
                "seed": seed, "train_fraction": split,
                "cv_folds": num_folds, "grid_size": len(spec["grid"]),
                "numeric_features": ",".join(numeric),
                "categorical_features": ",".join(categorical),
                **_hyperparams(model.stages[-1]),
            })
            mlflow.log_metrics(metrics)
            mlflow.log_metrics(per_class_metrics(preds, order))
            mlflow.spark.log_model(model, artifact_path="model")
            results.append({"name": name, "run_id": run.info.run_id, **metrics})

    best = max(results, key=lambda r: r["accuracy"])
    registered = mlflow.register_model(f"runs:/{best['run_id']}/model", model_name)
    model_uri = promote_best(client, model_name, registered.version)

    # Reload from the registry (not a path) and re-score to prove reproducibility.
    reloaded = mlflow.spark.load_model(model_uri)
    reload_metrics = evaluate(reloaded.transform(val_df))

    return {
        "fingerprint": fingerprint,
        "results": results,
        "best": best,
        "model_uri": model_uri,
        "reload_metrics": reload_metrics,
    }


def main() -> None:
    """Script entrypoint: python -m src.training (B.1 + B.2)."""
    import mlflow

    from src.utils.spark_session import get_spark

    config = load_config()
    mlflow_cfg = config.get("mlflow", {})
    if mlflow_cfg.get("tracking_uri"):
        mlflow.set_tracking_uri(mlflow_cfg["tracking_uri"])
    mlflow.set_experiment(mlflow_cfg.get("experiment", config["analysis"]["model_name"]))

    spark = get_spark(config)
    summary = train(spark, config)
    best = summary["best"]
    print(f"data_fingerprint={summary['fingerprint']}")
    for r in summary["results"]:
        print(f"  {r['name']}: acc={r['accuracy']:.3f} recall={r['weightedRecall']:.3f}")
    print(f"best={best['name']} -> {summary['model_uri']}")
    print(f"reloaded acc={summary['reload_metrics']['accuracy']:.3f}")
    spark.stop()


if __name__ == "__main__":
    main()
