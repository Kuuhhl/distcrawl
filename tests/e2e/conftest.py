import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Optional

import pytest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add modules to sys.path so they can be imported in all tests
root_dir = Path(__file__).parent.parent.parent
scripts_dir = root_dir / "scripts"
common_src_dir = root_dir / "common" / "src"
worker_src_dir = root_dir / "worker" / "src"

sys.path.insert(0, str(scripts_dir))
sys.path.insert(0, str(common_src_dir))
sys.path.insert(0, str(worker_src_dir))


@pytest.fixture(scope="session", autouse=True)
def pydantic_isolation():
    """Globally disable loading of .env files during test session."""
    os.environ["PYDANTIC_SETTINGS_ENV_FILE"] = "none"
    yield
    os.environ.pop("PYDANTIC_SETTINGS_ENV_FILE", None)


DOCKER_COMPOSE_FILE = "tests/e2e/docker-compose.yml"


class DockerCompose:
    """Wrapper for docker compose commands."""

    def __init__(self, file: str = DOCKER_COMPOSE_FILE):
        self.file = file

    def up(
        self,
        services: Optional[list] = None,
        detach: bool = True,
        build: bool = True,
        no_deps: bool = False,
        env: Optional[dict[str, str]] = None,
    ):
        """Run docker compose up."""
        cmd = ["docker", "compose", "-f", self.file, "up"]
        if build:
            cmd.append("--build")
        if detach:
            cmd.append("-d")
        if no_deps:
            cmd.append("--no-deps")
        if services:
            cmd.extend(services)

        current_env = os.environ.copy()
        if env:
            current_env.update(env)

        subprocess.run(cmd, check=True, env=current_env)

    def down(self):
        """Run docker compose down."""
        subprocess.run(
            ["docker", "compose", "-f", self.file, "down", "-v", "--remove-orphans"],
            check=True,
        )

    def stop(self, services: list):
        """Stop services."""
        subprocess.run(
            ["docker", "compose", "-f", self.file, "stop"] + services, check=True
        )

    def start(self, services: list):
        """Start services."""
        subprocess.run(
            ["docker", "compose", "-f", self.file, "start"] + services, check=True
        )

    def kill(self, services: list):
        """Kill services."""
        subprocess.run(
            ["docker", "compose", "-f", self.file, "kill"] + services, check=True
        )

    def logs(self, service: str):
        """Get service logs."""
        result = subprocess.run(
            ["docker", "compose", "-f", self.file, "logs", service],
            capture_output=True,
            text=True,
        )
        return result.stdout

    def is_running(self, service: str):
        """Check if service is running."""
        result = subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                self.file,
                "ps",
                "--status",
                "running",
                "-q",
                service,
            ],
            capture_output=True,
            text=True,
        )
        return bool(result.stdout.strip())


@pytest.fixture
def dc():
    """Fixture to manage docker compose life cycle."""
    # Environment isolation is handled by root tests/conftest.py

    composer = DockerCompose()
    composer.down()  # ensure clean state
    for d in [
        "tests/e2e/data_e2e",
    ]:
        if os.path.exists(d):
            try:
                shutil.rmtree(d)
            except PermissionError:
                # nats often writes files as root in alpine container
                subprocess.run(["sudo", "rm", "-rf", d], check=False)

    # recreate data dirs for bind-mounts
    os.makedirs("tests/e2e/data_e2e/nats", exist_ok=True)
    os.makedirs("tests/e2e/data_e2e/results", exist_ok=True)
    yield composer
