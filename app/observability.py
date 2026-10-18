import json
import logging
from pathlib import Path
from pyspark.sql.streaming import StreamingQueryListener

LOG = logging.getLogger(__name__)


class ProgressRecorder(StreamingQueryListener):
    def __init__(self, root):
        super().__init__()
        self.path = Path(root) / "observability" / "progress"
        self.path.mkdir(parents=True, exist_ok=True)

    def onQueryStarted(self, event):
        LOG.info("query_started name=%s id=%s", event.name, event.id)

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        target = self.path / f"{progress['name']}-{progress['runId']}-{progress['batchId']}.json"
        temp = target.with_suffix(".tmp")
        temp.write_text(json.dumps(progress, sort_keys=True))
        temp.replace(target)
        LOG.info("query_progress name=%s batch=%s rows=%s duration_ms=%s",
                 progress["name"], progress["batchId"], progress["numInputRows"],
                 progress.get("durationMs", {}).get("triggerExecution"))

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        LOG.info("query_terminated id=%s exception=%s", event.id, event.exception)
