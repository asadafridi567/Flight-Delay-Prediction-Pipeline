# DEBUG.md — Issues Encountered and How They Were Resolved

This document captures the real debugging journey of building this pipeline — not a sanitized "everything worked" account. Each entry includes the symptom, root cause, fix, and why it's worth understanding rather than just patching blindly. Organized roughly in the order these were hit, grouped by area.

---

## Data Ingestion

### 1. Lookup files downloading as HTML instead of CSV
**Symptom:** `DESCRIBE` on `airports_lookup`/`carriers_lookup` showed a single garbage column; inspecting the raw file showed `<!DOCTYPE html>`.
**Root cause:** BTS's `Download_Lookup.asp` endpoint returned a redirect/homepage instead of the actual file when hit via a plain scripted `requests.get()` — no browser-like headers, and the endpoint itself proved fragile to script against.
**Fix:** Switched to the OpenFlights GitHub-hosted `.dat` files (`airports.dat`, `airlines.dat`) — a stable, direct-download source with no redirect/session issues. Added a check on the response body (looking for `<!DOCTYPE`/`<html`) before trusting any download as real data, rather than assuming a 200 status code means success.
**Lesson:** A 200 status code does not guarantee correct content — verify the actual payload, especially against third-party endpoints not designed for scripted access.

### 2. `header=True` on files with no header row
**Symptom:** Loaded lookup tables had columns literally named `_c0`, `_c1`, etc., and the first real airport/airline record was missing.
**Root cause:** OpenFlights `.dat` files have no header row — every line is data. `header=True` caused Spark to consume the first real record as column names.
**Fix:** Changed to `header=False`, then explicitly renamed columns via `.toDF(...)` with the correct field names based on OpenFlights' documented schema.
**Lesson:** Never assume a CSV variant has a header — verify against the source's actual format documentation.

### 3. Column rename not persisting to the Delta table
**Symptom:** Even after adding `.toDF(...)` to rename columns, the saved table still showed `_c0`, `_c1`, etc.
**Root cause:** The rename was applied to a DataFrame read back *from the already-saved table* (which had already locked in bad column names at write time), not to the original CSV read — renaming in memory afterward doesn't retroactively change a table already persisted to Delta.
**Fix:** Applied `.toDF(...)` immediately after the CSV read, *before* the `.write.format("delta")` call — the corrected names are what actually get persisted.
**Lesson:** Delta locks in whatever schema exists at write time; any correction has to happen upstream of that write, not after.

### 4. `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES`
**Symptom:** Write failed citing invalid characters (spaces, parentheses, etc.) in column names.
**Root cause:** Source CSV headers sometimes contain characters Delta doesn't allow in column names.
**Fix:** A `clean_columns()` helper using regex substitution (`re.sub(r'[ ,;{}()\n\t=]', '_', c)`) applied to any dataset with a real header row before writing.
**Lesson:** Kept this function in the toolkit for any future source with a genuine header row — not needed for the OpenFlights files specifically, since those bypass header-based naming entirely via explicit `.toDF()`.

### 5. `DELTA_METADATA_MISMATCH` on reload
**Symptom:** Rewriting a lookup table after a previous failed/partial load errored with a schema mismatch.
**Root cause:** A prior failed attempt left a table with a different (invalid) schema; `mode("overwrite")` doesn't overwrite schema by default, only data.
**Fix:** `DROP TABLE IF EXISTS` before reloading, guaranteeing no leftover schema conflicts — more reliable than `.option("overwriteSchema", "true")` given how many failed attempts had already occurred.

### 6. Overwrite instead of append on monthly re-runs
**Symptom:** A single-run design (`mode("overwrite")`) would silently wipe all historical months every time the notebook ran, since the original version was written for a one-time bulk load only.
**Root cause:** The notebook wasn't designed for repeated monthly execution — it assumed one full load, not incremental accumulation.
**Fix:** Rebuilt the notebook to check which `(year, month)` combinations already exist in the table, download and load only missing months, and use `mode("append")` for every run after the first (`overwrite` only on true first-time table creation). Treats 404s as "BTS hasn't published this month yet" rather than an error, so a missed month is automatically picked up on the next run.
**Lesson:** A pipeline meant to run repeatedly needs idempotent, incremental logic from the start — retrofitting this after building single-run logic is more work than designing for it upfront.

---

## dbt

### 7. Databricks token missing required scope
**Symptom:** `dbt debug` failed with `Provided access token does not have required scopes: sql`.
**Root cause:** The Databricks PAT was generated without the scope needed to query a SQL Warehouse.
**Fix:** Regenerated the token using the "BI Tools" scope type (or manually including `sql` + `unity-catalog` scopes).

### 8. Malformed `profiles.yml` from manual editing
**Symptom:** `dbt init`/`dbt debug` failed with a YAML parser error pointing at a specific line/column.
**Root cause:** Manual hand-editing introduced an indentation inconsistency — YAML is whitespace-sensitive and a single misaligned space breaks parsing.
**Fix:** Deleted the file and regenerated it via a heredoc (`cat > profiles.yml << 'EOF' ... EOF`) to guarantee consistent formatting rather than hand-editing in a terminal.

### 9. Double-nested project folder from `dbt init`
**Symptom:** dbt project files landed at `dbt-project/flight_delay_prediction/flight_delay_prediction/` instead of directly under `dbt-project/`.
**Root cause:** Running `dbt init` from inside an already-created folder caused it to nest a new folder inside itself.
**Fix:** Ran `dbt init` from the parent directory so it created the project folder itself in one place, then flattened any resulting nesting with `mv`.

### 10. Schema doubling (`silver_silver`)
**Symptom:** Models configured with `+schema: silver` created a schema literally named `silver_silver` in Databricks.
**Root cause:** dbt's default schema-naming macro concatenates the profile's target schema with any custom `+schema` config (`<target_schema>_<custom_schema>`), and both were independently set to `silver`.
**Fix:** Overrode dbt's `generate_schema_name` macro to use the custom schema name exactly as specified, without prepending the target schema.

### 11. `dbt_utils.accepted_range` deprecation warning
**Symptom:** `MissingArgumentsPropertyInGenericTestDeprecation` warning on every run.
**Root cause:** Newer dbt versions expect generic test arguments nested under an `arguments:` key.
**Fix:** Restructured the YAML: `min_value: 0` moved under a new `arguments:` block.

---

## Dimensional Modeling (SCD2 / Star Schema)

### 12. `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE` on `carriers_snapshot`
**Symptom:** Snapshot MERGE failed; a `unique_dim_carriers_carrier_sk` test later also failed with 321 duplicate rows.
**Root cause:** IATA carrier codes get reused over decades as airlines shut down and codes are reassigned — the OpenFlights dataset has multiple `airline_id`s sharing the same `iata` code, violating the snapshot's assumed `unique_key`.
**Fix:** Added a `row_number()` window function in `stg_carriers` to keep exactly one row per `carrier_code` (preferring the currently active airline), deduplicating before the snapshot ever sees the data.
**Follow-up gotcha:** After fixing the staging dedup, the snapshot *still* showed duplicates — because the already-built `carriers_snapshot` table retained stale rows from before the fix (snapshots use incremental MERGE, not full replace). Required an explicit `DROP TABLE` on the snapshot (and everything downstream of it) before rebuilding clean.
**Lesson:** Fixing upstream logic doesn't retroactively clean already-materialized incremental/snapshot tables — they need an explicit rebuild.

### 13. SCD2 point-in-time join excluding almost all rows
**Symptom:** `not_null` tests on `fct_flights.origin_airport_sk`/`dest_airport_sk` failed for nearly every row (6.8M+ failures out of ~6.9M).
**Root cause:** The point-in-time join condition (`fl_date >= valid_from AND fl_date < valid_to`) compared 2025 flight dates against `dbt_valid_from`, which reflects *when the snapshot was actually run* (2026) — not any real-world date the dimension changed. Since the snapshot's capture date postdates every flight, the condition was false almost universally.
**Fix:** Changed the join to match the dimension's *current* version (`is_current = true`) instead of a strict point-in-time range — a deliberate, documented scoping decision appropriate for a one-time historical bulk load, rather than a bug to paper over.
**Lesson:** SCD2 point-in-time joins are only meaningful when the dimension's change history is dated within the same real-world period as the facts. A single bulk load doesn't naturally exercise that — worth explaining honestly rather than pretending the mechanism did something it didn't.

---

## Docker

### 14. `airflow: command not found` across every Airflow service
**Symptom:** `airflow-init`, `airflow-scheduler`, and `airflow-webserver` all failed identically, unable to find the `airflow` CLI at all — even though the base image alone (`docker run apache/airflow:2.9.3-python3.11 airflow version`) worked fine.
**Root cause:** A `pip install` dependency conflict during the custom image build — `dbt-databricks` requires `databricks-sql-connector>=3.5.0`, but the explicitly listed `databricks-sql-connector==2.9.6` (dragged in via Airflow's own constraints file when installed in the same `pip install` call as the Databricks provider) directly conflicted. Earlier attempts without `--constraint` failed silently rather than raising a clear error, producing a broken environment without any obvious cause in the build log.
**Fix:** Split the single `pip install` into two separate calls — one installing `apache-airflow-providers-databricks` under Airflow's official constraints file, and a second, unconstrained call installing `dbt-databricks` — so each package resolves its own dependencies independently instead of fighting over a shared `databricks-sql-connector` pin. Also removed the redundant explicit `databricks-sql-connector` line entirely, since `dbt-databricks` already pulls in a compatible version on its own.
**Lesson:** Always use `--constraint` when adding packages to an official Airflow image — it surfaces conflicts explicitly (as a build failure) instead of silently corrupting the environment. When one package's dependency directly conflicts with another, isolating them into separate `pip install` calls sidesteps the conflict entirely.

### 15. Stale images after rebuilding only one service
**Symptom:** After fixing the Dockerfile and rebuilding `airflow-init` specifically, `airflow-scheduler` and `airflow-webserver` still failed with the old error.
**Root cause:** Docker Compose doesn't automatically rebuild every service sharing a Dockerfile just because one was rebuilt — the other two were still running their old, broken images.
**Fix:** `docker compose build --no-cache` (no service name) to rebuild everything sharing that build context at once.

### 16. FastAPI `ModuleNotFoundError: No module named 'dotenv'`
**Symptom:** Container crashed on startup.
**Root cause:** `sql_agent.py` imported `python-dotenv` (leftover from local testing), which wasn't listed in `api/requirements.txt` — Docker Compose already injects environment variables directly, making the import both broken and unnecessary.
**Fix:** Removed the `dotenv` import/load call, since Compose's `environment:` block already handles this.

### 17. `GROQ_API_KEY` / Databricks vars not set inside containers
**Symptom:** `docker compose` warnings about missing environment variables; FastAPI failed to authenticate.
**Root cause:** `.env` file either didn't exist or wasn't populated — Codespace secrets live in the *shell environment*, not automatically in a file Compose can read.
**Fix:** Generated `.env` directly from the shell's existing environment variables (`echo $VAR` piped into a heredoc), avoiding manual retyping/copy-paste of sensitive values. Rebuilt this file after every Codespace restart, since Codespace secrets don't persist as a file across sessions.

---

## Airflow ↔ Databricks Integration

### 18. Databricks Job triggered with `job_id=0`
**Symptom:** `AirflowException: Job 0 does not exist`, despite the Airflow Variable holding the correct real Job ID.
**Root cause:** Passing the Job ID as a Jinja template string (`job_id="{{ var.value.ingest_flights_job_id }}"`) didn't resolve correctly for this operator/provider version — it silently evaluated to `0` instead of the real ID.
**Fix:** Resolved the Variable directly in Python at DAG parse time (`INGEST_FLIGHTS_JOB_ID = int(Variable.get("ingest_flights_job_id"))`) and passed the resulting integer directly to the operator, bypassing Jinja templating for this field entirely.
**Follow-up gotcha:** After adding the `Variable` import and the parse-time resolution, the DAG still failed the same way — because the actual `job_id=` line inside the operator still had the old templated string, never replaced. A reminder that adding a fix doesn't help if the old code path is still what's actually being executed.

### 19. `NameError: Variable is not defined`
**Symptom:** DAG failed to parse entirely after adding the `Variable.get(...)` fix.
**Root cause:** Missing `from airflow.models import Variable` import.
**Fix:** Added the import.

### 20. `NameError: name 'df' is not defined` inside the triggered Databricks Job
**Symptom:** The ingestion notebook, when run as a Databricks Job (not interactively), failed on its last cell.
**Root cause:** A leftover `df.display()` call from an earlier notebook version — the incremental rewrite had renamed the relevant variable to `df_new`, but the old debug-only display call was never removed.
**Fix:** Deleted the stale `df.display()` line.
**Lesson:** Debug/display-only cells that work fine interactively can break non-interactive Job execution if left in after a refactor — worth a final read-through before registering a notebook as a scheduled Job.

### 21. `dbt snapshot` failing with `[Errno 21] Is a directory`
**Symptom:** `dbt snapshot`, when run from inside the Airflow container (via a shared volume mount), failed trying to write a `.sql` file to a path that was actually a directory.
**Root cause:** The Airflow container's `dbt` version (pulled in via `requirements-dbt.txt`) differed from the version used interactively in the Codespace — two different dbt versions writing to the same shared, mounted `target/` build folder left it in an inconsistent state.
**Fix:** Deleted `dbt-project/target/` (safe — it's a regenerated build cache, already git-ignored) and reran.
**Lesson:** Running the same dbt project from two different environments with two different dbt versions against a shared filesystem path is fragile — pin versions consistently, or don't share the `target/` directory across environments.

### 22. Manually triggered DAG appearing to do nothing
**Symptom:** `airflow dags trigger` created a run, but no tasks visibly progressed.
**Root cause:** The DAG was in a **paused** state — new DAGs default to paused in Airflow as a safety measure, and a paused DAG's manually-triggered run doesn't reliably progress through the scheduler in all cases.
**Fix:** `airflow dags unpause flight_delay_pipeline` before triggering.

### 23. Airflow webserver 404 on internal links (Codespaces-specific)
**Symptom:** After logging in, some pages 404'd on a URL with `:8080` appended to a Codespaces forwarded URL that doesn't actually use that port in its visible address.
**Root cause:** Airflow's webserver wasn't proxy-aware, so it generated internal links assuming direct port access rather than accounting for being served through Codespaces' port-forwarding proxy.
**Fix:** Set `AIRFLOW__WEBSERVER__ENABLE_PROXY_FIX: "True"` (and, as a more explicit fallback, `AIRFLOW__WEBSERVER__BASE_URL` set to the actual forwarded Codespaces URL).

### 24. Login page appearing broken
**Symptom:** Initial login attempts failed with "Login Failed" in the webserver logs.
**Root cause:** Attempted login using an email address rather than the actual username (`admin`) created during `airflow-init`.
**Fix:** Used the correct `admin`/`admin` credentials from the `airflow users create` command.

### 25. Port 8080 showing a completely blank/unreachable page
**Root cause:** Codespaces' forwarded port visibility was set to Private rather than Public, blocking browser access entirely.
**Fix:** Changed port visibility to Public in the Codespaces "Ports" tab.

---

## Git / Version Control

### 26. Divergent branches on `git pull`
**Symptom:** `fatal: Need to specify how to reconcile divergent branches` after a PR merge added remote commits while local commits (a new README) existed independently.
**Root cause:** Local and remote `main` had each moved forward independently since the last sync.
**Fix:** `git config pull.rebase false` (merge strategy) + `git pull origin main`, resolving the resulting merge commit, then continuing with the new commits normally.

---

## Security

### 27. Databricks PAT pasted into chat multiple times
**Symptom:** A real access token was shared in plaintext across several messages while debugging authentication issues.
**Why this matters:** Regardless of where a credential is shared, once it's been transmitted outside of a secrets manager, it should be treated as compromised.
**Action taken:** Rotated the token — revoked the old one in Databricks, generated a new one, updated it everywhere it was referenced (Codespace secret, `.env`, Airflow connection).
**Lesson:** Reference credentials via environment variables (`$DATABRICKS_TOKEN`) in commands and documentation, never the literal value — including when asking for help debugging.

---

## Summary — what this debugging process demonstrates

Nearly every issue above falls into one of a few recurring categories: **silent failures that needed explicit verification** (HTML disguised as CSV, a dependency conflict masked until `--constraint` was added, a Jinja template silently resolving to `0`), **stale state from incremental tooling** (leftover snapshot rows, a shared `target/` directory, unrebuilt Docker images), and **environment/scoping mismatches** (SCD2 timestamps predating the data they're meant to track, Codespaces' proxy behavior). Recognizing and methodically isolating each of these — rather than guessing — is the actual skill this project exercised, arguably more than the individual fixes themselves.