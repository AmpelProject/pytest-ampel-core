import json
import subprocess
from os import environ
from pathlib import Path

import mongomock
import mongomock.collection
import pymongo
import pytest
import yaml
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import DEBUG, AmpelLogger


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
def testing_config():
    """Path to an Ampel config file suitable for testing."""
    return Path(__file__).parent / "test-data" / "testing-config.yaml"


@pytest.fixture
def mock_context(testing_config: Path, tmp_path, _mongomock):
    """An AmpelContext with a mongomock backend."""
    # remove storageEngine options that are not supported by mongomock
    with open(testing_config) as f:
        config = yaml.safe_load(f)
        for db in config["mongo"]["databases"]:
            for collection in db["collections"]:
                if "args" in collection and "storageEngine" in collection["args"]:
                    collection["args"].pop("storageEngine")
    sanitized_config = tmp_path / "sanitized-testing-config.yaml"
    with open(sanitized_config, "w") as f:
        yaml.safe_dump(config, f)
        return DevAmpelContext.load(config=str(sanitized_config), purge_db=True)


@pytest.fixture
def integration_context(testing_config: Path, _mongod):
    """An AmpelContext connected to a real MongoDB instance."""
    ctx = DevAmpelContext.load(
        config=str(testing_config),
        purge_db=True,
        custom_conf={"resource.mongo": _mongod},
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
