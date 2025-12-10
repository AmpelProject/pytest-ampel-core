import json
import subprocess
from os import environ
from pathlib import Path

import mongomock
import mongomock.collection
import pymongo
import pytest

from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.log.AmpelLogger import AmpelLogger, DEBUG


def pytest_addoption(parser):
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run docker-based integration tests",
    )


@pytest.fixture(scope="session")
def mongod(pytestconfig):
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
def _patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.core.AmpelDB.MongoClient", mongomock.MongoClient)
    # ignore codec_options in DataLoader
    monkeypatch.setattr("mongomock.codec_options.is_supported", lambda *args: None)
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
    return Path(__file__).parent / "test-data" / "testing-config.yaml"


@pytest.mark.usefixtures("_patch_mongo")
@pytest.fixture
def mock_context(testing_config: Path):
    return DevAmpelContext.load(config=str(testing_config), purge_db=True)


@pytest.fixture
def integration_context(mongod, testing_config: Path):
    ctx = DevAmpelContext.load(
        config=str(testing_config),
        purge_db=True,
        custom_conf={"resource.mongo": mongod},
    )
    yield ctx
    ctx.db.close()


# metafixture as suggested in https://github.com/pytest-dev/pytest/issues/349#issuecomment-189370273
@pytest.fixture(params=["mock_context", "integration_context"])
def dev_context(request):
    return request.getfixturevalue(request.param)


@pytest.fixture
def ampel_logger():
    return AmpelLogger.get_logger(console=dict(level=DEBUG))
