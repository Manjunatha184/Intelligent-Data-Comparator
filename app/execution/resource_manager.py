from dataclasses import dataclass

from app.execution.models import ExecutionTask


@dataclass(frozen=True)
class ResourceCapacity:
    """
    Total resources available to the execution engine.
    """

    cpu_units: int
    memory_mb: int
    workers: int


@dataclass(frozen=True)
class ResourceRequirement:
    """
    Resources required by one execution task.
    """

    cpu_units: int = 1
    memory_mb: int = 256
    workers: int = 1


class ResourceManager:
    """
    Tracks resources currently allocated to running tasks.
    """

    def __init__(
        self,
        capacity: ResourceCapacity,
    ):
        if capacity.cpu_units <= 0:
            raise ValueError(
                "cpu_units must be greater than zero."
            )

        if capacity.memory_mb <= 0:
            raise ValueError(
                "memory_mb must be greater than zero."
            )

        if capacity.workers <= 0:
            raise ValueError(
                "workers must be greater than zero."
            )

        self.capacity = capacity

        self.used_cpu = 0
        self.used_memory_mb = 0
        self.used_workers = 0

    def can_allocate(
        self,
        requirement: ResourceRequirement,
    ) -> bool:

        return (
            self.used_cpu
            + requirement.cpu_units
            <= self.capacity.cpu_units
            and
            self.used_memory_mb
            + requirement.memory_mb
            <= self.capacity.memory_mb
            and
            self.used_workers
            + requirement.workers
            <= self.capacity.workers
        )

    def allocate(
        self,
        requirement: ResourceRequirement,
    ) -> None:

        if not self.can_allocate(requirement):
            raise RuntimeError(
                "Insufficient execution resources."
            )

        self.used_cpu += (
            requirement.cpu_units
        )

        self.used_memory_mb += (
            requirement.memory_mb
        )

        self.used_workers += (
            requirement.workers
        )

    def release(
        self,
        requirement: ResourceRequirement,
    ) -> None:

        self.used_cpu -= (
            requirement.cpu_units
        )

        self.used_memory_mb -= (
            requirement.memory_mb
        )

        self.used_workers -= (
            requirement.workers
        )

        if (
            self.used_cpu < 0
            or self.used_memory_mb < 0
            or self.used_workers < 0
        ):
            raise RuntimeError(
                "Resource accounting became negative."
            )

    def available_resources(self) -> ResourceCapacity:

        return ResourceCapacity(
            cpu_units=(
                self.capacity.cpu_units
                - self.used_cpu
            ),
            memory_mb=(
                self.capacity.memory_mb
                - self.used_memory_mb
            ),
            workers=(
                self.capacity.workers
                - self.used_workers
            ),
        )