import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    lake_root: str = os.getenv("LAKE_ROOT", "/lake")
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    trigger_seconds: int = int(os.getenv("TRIGGER_SECONDS", "10"))
    max_offsets: int = int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "20000"))
    max_files: int = int(os.getenv("MAX_FILES_PER_TRIGGER", "32"))
    shuffle_partitions: int = int(os.getenv("SHUFFLE_PARTITIONS", "4"))

    def __post_init__(self):
        if "`" in self.lake_root or not Path(self.lake_root).is_absolute():
            raise ValueError("LAKE_ROOT must be an absolute local path without backticks")
        if min(self.trigger_seconds, self.max_offsets, self.max_files, self.shuffle_partitions) < 1:
            raise ValueError("Rate limits, trigger interval and partitions must be positive")

    def path(self, name):
        return str(Path(self.lake_root) / name)
