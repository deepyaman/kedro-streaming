import logging
import sys
from typing import Any, Dict

import git
import mlflow
from kedro.io.core import AbstractDataSet
from kedro.io.data_catalog import DataCatalog
from kedro.pipeline import Pipeline
from mlflow.models.signature import ModelSignature
from mlflow.tracking import MlflowClient


class MLFlowDataSet(AbstractDataSet):
    _kedro_run_params = None
    _kedro_pipeline = None
    _kedro_catalog = None

    def __init__(
        self,
        tracking_uri: str,
        experiment_name: str,
        model_name: str,
        env: str = None,
        model_version: str = None,
        signature: Dict[str, Dict[str, str]] = None,
        input_example: Dict[str, Any] = None,
    ):
        self._tracking_uri = tracking_uri
        if tracking_uri:
            print(f"uri: {tracking_uri}")
            mlflow.set_tracking_uri(tracking_uri)
            mlflow.set_registry_uri(tracking_uri)

        self._experiment_name = experiment_name
        self._model_name = model_name

        if env is not None:
            if model_version is not None:
                logger = logging.getLogger(__name__)
                logger.warning(
                    "env and version should not be specified together. "
                    "Ignoring version, using env"
                )
                model_version = None

        self._version = model_version
        self._env = env

        if signature:
            self._signature = ModelSignature.from_dict(signature)
        else:
            self._signature = None
        self._input_example = input_example

    def _save(self, model: Any) -> None:
        mlflow.set_experiment(self._experiment_name)
        if not mlflow.active_run():
            mlflow.start_run()
        mlflow.sklearn.log_model(
            model,
            self._model_name,
            registered_model_name=self._model_name,
            signature=self._signature,
            input_example=self._input_example,
        )
        git_sha = get_git_sha()
        if git_sha is not None:
            mlflow.log_param("git_sha", git_sha)

        _log_kedro_info(
            self._kedro_run_params, self._kedro_pipeline, self._kedro_catalog
        )

    def _load(self) -> Any:
        *_, latest_version = parse_model_uri(f"models:/{self._model_name}")

        model_version = self._version or latest_version
        if self._env is not None:
            *_, model_version = parse_model_uri(
                f"models:/{self._model_name}/{self._env}"
            )

        logger = logging.getLogger(__name__)

        logger.info(f"Loading model '{self._model_name}' version '{model_version}'")

        if model_version != latest_version:
            if self._env is not None:
                logger.warning(f"{self._env} environment has older version.")
            logger.warning(f"Newer version {latest_version} exists in repo")

        model = mlflow.sklearn.load_model(f"models:/{self._model_name}/{model_version}")

        return model

    def _describe(self) -> Dict[str, Any]:
        return dict(
            tracking_uri=self._tracking_uri,
            experiment_name=self._experiment_name,
            model_name=self._model_name,
            env=self._env,
            model_version=self._version,
            signature=self._signature,
            input_example=self._input_example,
        )


def _log_kedro_info(
    run_params: Dict[str, Any], pipeline: Pipeline, catalog: DataCatalog
) -> None:
    # this will have all the nested structures (duplicates)
    parameters = {
        input_param: catalog._data_sets[input_param].load()
        for input_param in pipeline.inputs()
        if "param" in input_param
    }

    # similar to context.params
    parameters.update(run_params.get("extra_params", {}))
    parameters_artifacts = {
        f"kedro_{_sanitise_kedro_param(param_name)}": param_value
        for param_name, param_value in parameters.items()
    }

    mlflow.log_dict(
        {
            # could use `run_params` in the future
            "kedro_run_args": " ".join(
                repr(a) if " " in a else a for a in sys.argv[1:]
            ),
            "kedro_nodes": sorted(n.short_name for n in pipeline.nodes),
            "kedro_dataset_versions": list(_get_dataset_versions(catalog, pipeline)),
            **parameters_artifacts,
        },
        "kedro.yaml",
    )


def _sanitise_kedro_param(param_name):
    sanitised_param_name = param_name.replace(":", "_")
    return sanitised_param_name


def _get_dataset_versions(catalog: DataCatalog, pipeline: Pipeline):
    for ds_name, ds in sorted(catalog._data_sets.items()):
        ds_in_out = ds_name in pipeline.all_outputs()
        try:
            save_ver = ds.resolve_save_version() if ds_in_out else None
            load_ver = ds.resolve_save_version() if ds_in_out else None
        except AttributeError:
            save_ver = None
            load_ver = None
        if save_ver or load_ver:
            version_info = {
                "name": ds_name,
                "save_version": save_ver,
                "load_version": load_ver,
            }
            yield version_info


def get_git_sha():
    try:
        git_repo = git.Repo(search_parent_directories=True)
        return git_repo.head.object.hexsha
    except Exception as exc:
        logger = logging.getLogger(__name__)
        logger.error("Unable to get 'git_sha'")
        logger.error(str(exc))
    return None


def parse_model_uri(model_uri):
    parts = model_uri.split("/")

    if len(parts) < 2:
        raise ValueError(
            f"model uri should have the format "
            f"'models:/<model_name>' or "
            f"'models:/<model_name>/<version>', got {model_uri}"
        )

    if parts[0] == "models:":
        protocol = "models"
    else:
        raise ValueError("model uri should start with `models:/`, got %s", model_uri)

    name = parts[1]

    client = MlflowClient()
    if len(parts) == 2:
        results = client.search_model_versions(f"name='{name}'")
        latest_version = results[-1].version
        version = latest_version
    else:
        version = parts[2]
        if version in ["Production", "Staging", "Archived"]:
            results = client.get_latest_versions(name, stages=[version])
            version = results[0].version

    return protocol, name, version
