import json
import subprocess
from os import environ
from pathlib import Path

import mongomock
import mongomock.collection
import pymongo
import pytest
import yaml
from ampel.config.builder.DisplayOptions import DisplayOptions
from ampel.config.builder.DistConfigBuilder import DistConfigBuilder
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import DEBUG, AmpelLogger
from ampel.secret.AmpelVault import AmpelVault
from ampel.secret.PotemkinSecretProvider import PotemkinSecretProvider


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run docker-based integration tests",
    )


@pytest.fixture(scope="session")
def _mongod(pytestconfig):
    if port := environ.get("MONGO_PORT"):
        yield f"mongodb://localhost:{port}"
        return

    if not pytestconfig.getoption("--integration"):
        raise pytest.skip("integration tests require --integration flag")
    try:
        container = (
            subprocess.check_output(["docker", "run", "--rm", "-d", "-P", "mongo:8"])
            .decode()
            .strip()
        )
    except FileNotFoundError:
        pytest.skip("integration tests require docker")
        return
    try:
        port = json.loads(subprocess.check_output(["docker", "inspect", container]))[0][
            "NetworkSettings"
        ]["Ports"]["27017/tcp"][0]["HostPort"]
        # wait for startup
        with pymongo.MongoClient(port=int(port)) as client:
            list(client.list_databases())
        yield f"mongodb://localhost:{port}"
    finally:
        ...
        subprocess.check_call(["docker", "stop", container])


@pytest.fixture
def _mongomock(monkeypatch):
    monkeypatch.setattr("ampel.core.AmpelDB.MongoClient", mongomock.MongoClient)
    # ignore codec_options in DataLoader
    monkeypatch.setattr("mongomock.codec_options.is_supported", lambda *args: None)  # noqa: ARG005
    # work around https://github.com/mongomock/mongomock/issues/912
    add_update = mongomock.collection.BulkOperationBuilder.add_update

    def _add_update(self, *args, sort=None, **kwargs):
        if sort is not None:
            raise NotImplementedError("sort not implemented in mongomock")
        return add_update(self, *args, **kwargs)

    monkeypatch.setattr(
        "mongomock.collection.BulkOperationBuilder.add_update", _add_update
    )


@pytest.fixture(scope="session")
def testing_config(tmp_path_factory):
    """Path to an Ampel config file suitable for testing."""
    config_path = tmp_path_factory.mktemp("config") / "testing-config.yaml"
    # build a config from all available ampel distributions
    cb = DistConfigBuilder(
        DisplayOptions(verbose=False, debug=False),
    )
    cb.load_distributions()
    config = cb.build_config(
        stop_on_errors=2,
        config_validator="ConfigValidator",
        get_unit_env=False,
    )
    assert config is not None
    # remove storageEngine options that are not supported by mongomock
    for db in config["mongo"]["databases"]:
        for collection in db["collections"]:
            if "args" in collection and "storageEngine" in collection["args"]:
                collection["args"].pop("storageEngine")
    with open(config_path, "w") as f:
        yaml.safe_dump(config, f)
    return config_path


@pytest.fixture
def mock_context(testing_config: Path, ampel_vault: AmpelVault, _mongomock):
    """An AmpelContext with a mongomock backend."""
    return DevAmpelContext.load(
        config=str(testing_config),
        vault=ampel_vault,
        purge_db=True,
    )


@pytest.fixture
def integration_context(testing_config: Path, ampel_vault: AmpelVault, _mongod):
    """An AmpelContext connected to a real MongoDB instance."""
    ctx = DevAmpelContext.load(
        config=str(testing_config),
        purge_db=True,
        custom_conf={"resource.mongo": _mongod},
        vault=ampel_vault,
    )
    yield ctx
    ctx.db.close()


# metafixture as suggested in https://github.com/pytest-dev/pytest/issues/349#issuecomment-189370273
@pytest.fixture(params=["mock_context", "integration_context"])
def dev_context(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def ampel_logger():
    """An AmpelLogger instance with DEBUG level console output."""
    return AmpelLogger.get_logger(console=dict(level=DEBUG))


@pytest.fixture
def ampel_vault():
    """An AmpelVault instance configured with PotemkinSecretProvider."""
    return AmpelVault([PotemkinSecretProvider()])
