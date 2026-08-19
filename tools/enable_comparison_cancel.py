from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
backend = ROOT / "app/api/routes/comparisons.py"
schema = ROOT / "app/api/schemas/comparison.py"
frontend = ROOT / "frontend/src/main.jsx"

# ---------------- backend comparisons route ----------------
text = backend.read_text()
text = text.replace(
    "from fastapi import APIRouter, HTTPException, Query",
    "from fastapi import APIRouter, BackgroundTasks, HTTPException, Query",
)

# Honor a cancellation request that arrived immediately after the run was created.
needle = '''    RUNS[run_id]["engine"] = engine
    RUNS[run_id]["plan"] = plan

    # --------------------------------------------------------
    # Initial scheduling
'''
replacement = '''    RUNS[run_id]["engine"] = engine
    RUNS[run_id]["plan"] = plan

    if RUNS[run_id].get("cancel_requested"):
        engine.request_cancel()

    # --------------------------------------------------------
    # Initial scheduling
'''
if needle not in text:
    raise SystemExit("execute_plan insertion point not found")
text = text.replace(needle, replacement, 1)

# Add safe background wrapper before CREATE COMPARISON.
marker = '''# ============================================================
# CREATE COMPARISON
# ============================================================
'''
helper = '''def _execute_plan_background(run_id: str, plan) -> None:
    try:
        execute_plan(run_id, plan)
    except Exception as exc:
        logger.exception("Comparison execution failed for run %s", run_id)
        run = RUNS.get(run_id)
        if run is not None:
            run["execution_error"] = str(exc)


# ============================================================
# CREATE COMPARISON
# ============================================================
'''
if marker not in text:
    raise SystemExit("create marker not found")
text = text.replace(marker, helper, 1)

text = text.replace(
    '''def create_comparison(
    request: ComparisonRequest,
):''',
    '''def create_comparison(
    request: ComparisonRequest,
    background_tasks: BackgroundTasks,
):''',
    1,
)

# Add cancellation/error state to the in-memory run.
text = text.replace(
    '''            "runtime_queue": None,
            "result": None,
        }''',
    '''            "runtime_queue": None,
            "result": None,
            "cancel_requested": False,
            "execution_error": None,
        }''',
    1,
)

# Replace blocking execution + post-execution status calculation with immediate start response.
pattern = re.compile(
    r'''\n        execute_plan\(\n            run_id,\n            plan,\n        \)\n\n        runtime_queue = \(\n            RUNS\[run_id\]\["runtime_queue"\]\n        \)\n.*?        return ComparisonStartResponse\(\n            run_id=run_id,\n            plan_id=plan\.metadata\.plan_id,\n            status=status,\n            total_tasks=len\(plan\.tasks\),\n            task_ids=\[\n                task\.task_id\n                for task in plan\.tasks\n            \],\n        \)''',
    re.S,
)
new = '''
        background_tasks.add_task(
            _execute_plan_background,
            run_id,
            plan,
        )

        return ComparisonStartResponse(
            run_id=run_id,
            plan_id=plan.metadata.plan_id,
            status="RUNNING",
            total_tasks=len(plan.tasks),
            task_ids=[
                task.task_id
                for task in plan.tasks
            ],
        )'''
text, count = pattern.subn(new, text, count=1)
if count != 1:
    raise SystemExit("blocking create_comparison block not found")

# Status before engine initialization can still represent an accepted cancel request.
text = text.replace(
    '''        return ComparisonStatusResponse(
            run_id=run_id,
            plan_id=plan.metadata.plan_id,
            status="PENDING",
        )''',
    '''        return ComparisonStatusResponse(
            run_id=run_id,
            plan_id=plan.metadata.plan_id,
            status=(
                "CANCEL_REQUESTED"
                if run.get("cancel_requested")
                else "PENDING"
            ),
        )''',
    1,
)

# Include CANCELLED task states in terminal progress/status calculation.
needle = '''    total_tasks = len(plan.tasks)

    finished_tasks = (
        len(completed)
        + len(failed)
    )'''
replacement = '''    total_tasks = len(plan.tasks)

    cancelled_count = sum(
        1
        for state in runtime_queue.task_states.values()
        if _state_status_value(state) == "CANCELLED"
    )

    finished_tasks = (
        len(completed)
        + len(failed)
        + cancelled_count
    )'''
if needle not in text:
    raise SystemExit("status progress block not found")
text = text.replace(needle, replacement, 1)

old_status = '''    status = (
        "FAILED"
        if failed
        else (
            "COMPLETED"
            if finished_tasks == total_tasks
            else "RUNNING"
        )
    )'''
new_status = '''    if run.get("execution_error"):
        status = "FAILED"
    elif run.get("cancel_requested"):
        status = (
            "CANCELLED"
            if not running and finished_tasks == total_tasks
            else "CANCEL_REQUESTED"
        )
    else:
        status = (
            "FAILED"
            if failed
            else (
                "COMPLETED"
                if finished_tasks == total_tasks
                else "RUNNING"
            )
        )'''
if old_status not in text:
    raise SystemExit("overall status block not found")
text = text.replace(old_status, new_status, 1)

# Existing cancel endpoint now handles the tiny race before engine initialization.
old_cancel = '''    engine = run.get("engine")

    if engine is None:

        raise HTTPException(
            status_code=400,
            detail="Execution has not started.",
        )

    engine.request_cancel()

    return ComparisonCancelResponse('''
new_cancel = '''    run["cancel_requested"] = True

    engine = run.get("engine")

    if engine is not None:
        engine.request_cancel()

    return ComparisonCancelResponse('''
if old_cancel not in text:
    raise SystemExit("cancel endpoint block not found")
text = text.replace(old_cancel, new_cancel, 1)
backend.write_text(text)

# ---------------- frontend ----------------
ui = frontend.read_text()

# Track the active run so the modal can cancel it while runComparison polls.
needle = '''  const [running, setRunning] = useState(false);
'''
replacement = '''  const [running, setRunning] = useState(false);
  const [activeRunId, setActiveRunId] = useState(null);
  const [cancelRequested, setCancelRequested] = useState(false);
'''
if needle not in ui:
    raise SystemExit("frontend running state not found")
ui = ui.replace(needle, replacement, 1)

# Reset run/cancel state just before starting.
ui = ui.replace(
    '''    setRunning(true);

    try {''',
    '''    setRunning(true);
    setActiveRunId(null);
    setCancelRequested(false);

    try {''',
    1,
)

# Replace synchronous completion handling with status polling.
old = '''      notify(
        `Comparison ${String(
          result.status
        ).toLowerCase()}.`
      );

      onComplete(result.run_id);
'''
new = '''      setActiveRunId(result.run_id);

      while (true) {
        await new Promise((resolve) => window.setTimeout(resolve, 1000));
        const statusResult = await apiRequest(`/comparisons/${result.run_id}`);
        const executionStatus = String(statusResult.status || "").toUpperCase();

        if (executionStatus === "COMPLETED" || executionStatus === "FAILED") {
          notify(`Comparison ${executionStatus.toLowerCase()}.`);
          onComplete(result.run_id);
          break;
        }

        if (executionStatus === "CANCELLED") {
          notify("Comparison cancelled.");
          setReviewModalOpen(false);
          break;
        }
      }
'''
if old not in ui:
    raise SystemExit("frontend completion block not found")
ui = ui.replace(old, new, 1)

# Add the actual cancellation handler before the builder return.
marker = '''  return (
    <div className="stack comparisonBuilder">'''
handler = '''  async function cancelComparison() {
    if (!activeRunId || cancelRequested) return;
    setCancelRequested(true);
    try {
      await apiRequest(`/comparisons/${activeRunId}/cancel`, { method: "POST" });
      notify("Cancellation requested.");
    } catch (error) {
      setCancelRequested(false);
      notify(error.message, "error");
    }
  }

  return (
    <div className="stack comparisonBuilder">'''
if marker not in ui:
    raise SystemExit("builder return marker not found")
ui = ui.replace(marker, handler, 1)

# Pass cancellation controls to ReviewModal.
needle = '''          onRun={runComparison}
          running={running}
        />'''
replacement = '''          onRun={runComparison}
          running={running}
          onCancel={cancelComparison}
          canCancel={Boolean(activeRunId)}
          cancelRequested={cancelRequested}
        />'''
if needle not in ui:
    raise SystemExit("ReviewModal call not found")
ui = ui.replace(needle, replacement, 1)

# Extend ReviewModal props.
ui = ui.replace(
    '''  onRun,
  running,
}) {''',
    '''  onRun,
  running,
  onCancel,
  canCancel,
  cancelRequested,
}) {''',
    1,
)

# Turn the formerly-disabled running Cancel button into the real cancellation action.
old = '''          <button type="button" className="secondary" onClick={onClose} disabled={running}>
            Cancel
          </button>'''
new = '''          <button
            type="button"
            className="secondary"
            onClick={running ? onCancel : onClose}
            disabled={running && (!canCancel || cancelRequested)}
          >
            {cancelRequested ? "Cancelling…" : running ? "Cancel comparison" : "Cancel"}
          </button>'''
if old not in ui:
    raise SystemExit("ReviewModal cancel button not found")
ui = ui.replace(old, new, 1)

frontend.write_text(ui)
print("Enabled asynchronous comparison execution and real cancellation UI")
