using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Exporter.Run;

public sealed record RunOptions(bool PlanOnly = false, DateTime? NowUtc = null);

/// <summary>
/// `darling-export run`: the scheduler (R1-R26). One run: the restart delay, the lock, the families in order, the
/// post-run hook, then the daily-log fold and cleanup. Exit codes: 0 = ok, skip, or lock busy; 1 = a failure; 2 =
/// refused. Plan-only shares the planner but takes no lock, writes no state, daily log, or cleanup, runs no hook and
/// never starts a child; it appends one JSON line to `plan-only-&lt;store&gt;-yyyyMMdd.jsonl`.
/// </summary>
public sealed class RunScheduler(
    RunSettings settings, RunOptions options, Func<DateTime> clock,
    IChildRunner childRunner, IRestartDetector restart, IHostMemory memory, TextWriter output)
{
    public const int ExitOk = 0, ExitFailed = 1, ExitRefused = 2;

    private sealed record CallRecord(string Family, string LogPath);

    public async Task<int> RunAsync(CancellationToken ct)
    {
        if (settings.Validate() is { } refusal)
        {
            output.WriteLine($"refused: {refusal}");
            return ExitRefused;
        }

        foreach (var warning in settings.Warnings())
        {
            output.WriteLine($"warning: {warning}");
        }

        var nowUtc = (options.NowUtc ?? clock()).ToUniversalTime();
        var (loaded, stateError) = ScheduleState.Load(settings.StatePath);
        if (options.PlanOnly) return PlanOnly(nowUtc, loaded, stateError);

        if (stateError is not null)
        {
            output.WriteLine(stateError);
            return ExitRefused;
        }

        // The cutoff clock follows --now plus real elapsed time (R25): the offset is about 0 in production.
        var clockOffset = nowUtc - clock();
        var dailyLogPath = RunFiles.DailyLogPath(settings.LogDir, settings.Store, nowUtc);

        // R17: before the lock, any child call, and any state write.
        if (settings.Restart.DelayMinutes > 0)
        {
            var restartUtc = restart.RestartUtc();
            if (RestartDelay.IsWithin(nowUtc, restartUtc, settings.Restart.DelayMinutes))
            {
                var minutesAgo = (int)Math.Floor((nowUtc - restartUtc!.Value).TotalMinutes);
                var message = string.Format(CultureInfo.InvariantCulture,
                    "skipped: Darling restarted at {0} ({1} min ago), inside the {2}-minute restart delay; the next run catches up",
                    ScheduleState.Stamp(restartUtc.Value), minutesAgo, settings.Restart.DelayMinutes);
                try { RunFiles.AppendDaily(dailyLogPath, RunFiles.SkipBlock(nowUtc, settings.Store, message)); }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
                output.WriteLine(message);
                return ExitOk;
            }
        }

        Directory.CreateDirectory(settings.LogDir);
        Directory.CreateDirectory(settings.OutDir);
        using var runLock = RunLock.TryAcquire(settings.StateDir);
        if (runLock is null)
        {
            output.WriteLine("another run holds the lock; exiting");
            return ExitOk;
        }

        var stopwatch = Stopwatch.StartNew();
        var calls = new List<CallRecord>();
        PostRunResult? hookResult = null;
        var failed = false;
        var completed = false;
        try
        {
            var state = loaded ?? ScheduleState.NewCold(settings.Store, settings.Families.QueryStoreEnabled, settings.Schedule.StartUtc, clock());
            state.Root["last_attempt_utc"] = ScheduleState.Stamp(nowUtc);
            state.Save(settings.StatePath);

            failed = await RunFamiliesAsync(state, nowUtc, clockOffset, dailyLogPath, calls, ct).ConfigureAwait(false);

            // R24: always runs, and its failure never writes export state.
            if (settings.PostRun is { } hook)
            {
                output.WriteLine($"running {hook.Name} for {settings.Store}");
                hookResult = await PostRunHook.RunAsync(hook, settings.LogDir, nowUtc, ct).ConfigureAwait(false);
                if (hookResult.ExitCode != 0)
                {
                    output.WriteLine($"{hook.Name} failed (exit {hookResult.ExitCode}); see {hookResult.LogPath}");
                    failed = true;
                }
            }
            completed = true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            output.WriteLine($"run failed: {ex.Message}");
            failed = true;
        }
        finally
        {
            // The fold happens while the lock is still held, so two runs never interleave blocks (R21).
            var runExit = completed && !failed ? 0 : 1;
            FoldDailyLog(dailyLogPath, nowUtc, calls, hookResult, runExit, stopwatch.Elapsed.TotalSeconds);
            Cleanup(nowUtc);
        }
        return failed ? ExitFailed : ExitOk;
    }

    // ---- the families loop (R2, R7-R9, R12-R15) ----

    private async Task<bool> RunFamiliesAsync(ScheduleState state, DateTime nowUtc, TimeSpan clockOffset, string dailyLogPath, List<CallRecord> calls, CancellationToken ct)
    {
        var failed = false;
        var optionalTables = ReadOptionalTables();
        output.WriteLine($"optional tables: {(optionalTables.Count > 0 ? string.Join(',', optionalTables) : "none")}");
        var mode = Planner.ResolveMode(settings, optionalTables);

        // R12: fresh_fast state follows the window size.
        if (mode.UseFast)
        {
            if (!state.Has(Planner.FreshFast)) state.Root[Planner.FreshFast] = ScheduleState.NewBlock(state.LastTo(Planner.Fresh));
        }
        else if (settings.Families.FastWindowMinutes == 60 && state.Root.ContainsKey(Planner.FreshFast))
        {
            state.Remove(Planner.FreshFast);
            state.Save(settings.StatePath);
            output.WriteLine("fresh_fast state dropped (the window size is 60); fresh re-exports from its own last_to");
        }

        if (mode.UseFast) output.WriteLine($"late-row margin for fresh_fast: {mode.LateMargin} minutes ({mode.LateSource})");
        if (mode.HasQueryStore) output.WriteLine($"query_store lag: {mode.QueryStoreLag} hours ({mode.QueryStoreLagSource})");

        foreach (var family in Planner.FamilyOrder(mode))
        {
            var qsEarly = Planner.IsQueryStoreEarly(family, mode);
            var plan = Planner.PlanFamily(settings, mode, family, state.LastTo(family), nowUtc);
            if (plan.Status == "waits_for_minute_20")
            {
                output.WriteLine($"hourly family {family} waits for minute 20");
                continue;
            }
            if (plan.Windows.Count == 0)
            {
                output.WriteLine($"nothing due for {family}");
                continue;
            }

            foreach (var w in plan.Windows)
            {
                // R7: the cutoff keeps a slow hourly run out of the next hour's :20 run. The clock is read again before each window.
                if (family != Planner.FreshFast && !qsEarly && (clock() + clockOffset).Minute >= settings.Schedule.CutoffMinute)
                {
                    output.WriteLine($"past minute {settings.Schedule.CutoffMinute}; not starting any more windows for {family} this run");
                    break;
                }

                var block = state.Block(family);
                if (w.IsGap)
                {
                    state.Array("gaps").Add(new JsonObject
                    {
                        ["family"] = family, ["from"] = ScheduleState.Stamp(w.From), ["to"] = ScheduleState.Stamp(w.To),
                        ["recorded_utc"] = ScheduleState.Stamp(clock()),
                    });
                    block["last_to"] = ScheduleState.Stamp(w.To);
                    state.Save(settings.StatePath);
                    output.WriteLine($"recorded gap for {family} {ScheduleState.Stamp(w.From)} .. {ScheduleState.Stamp(w.To)}; not exported");
                    continue;
                }

                var call = Planner.CallFor(mode, plan, w);
                output.WriteLine($"exporting {settings.Store} {family} {call.FromArg} .. {call.ToArg}");
                var (result, runError) = await InvokeChildAsync(call, ct).ConfigureAwait(false);
                if (result is { NotStarted: null })
                {
                    calls.Add(new CallRecord(family, result.LogPath));
                    RecordRun(state, block, family, result, dailyLogPath);
                }

                var exit = result is { NotStarted: null } ? result.ExitCode : (int?)null;
                if (exit == 3) RecordOptionalFailures(state, call, plan, result!);

                if (exit is 0 or 3)
                {
                    block["last_to"] = ScheduleState.Stamp(w.To);
                    block["last_success_utc"] = ScheduleState.Stamp(clock());
                    block["last_error"] = null;
                    state.Save(settings.StatePath);
                }
                else
                {
                    block["last_failure_utc"] = ScheduleState.Stamp(clock());
                    block["last_error"] = FailureText(result, runError, "window");
                    state.Save(settings.StatePath);
                    output.WriteLine($"export failed for {family} {call.FromArg} .. {call.ToArg}; stopping this family for this run");
                    failed = true;
                    break;
                }
            }
        }
        return failed;
    }

    private async Task<(ChildResult? Result, string? Error)> InvokeChildAsync(PlannedCall call, CancellationToken ct)
    {
        try
        {
            Directory.CreateDirectory(settings.LogDir);
            var stamp = RunFiles.RunStamp(clock());
            var log = Path.Combine(settings.LogDir, $"export-{settings.Store}-{stamp}.log");
            for (var n = 2; File.Exists(log) || File.Exists(log + ".err"); n++)
            {
                log = Path.Combine(settings.LogDir, $"export-{settings.Store}-{stamp}_{n}.log");
            }
            var childCall = new ChildCall(settings.ResolvedChildExe, Planner.BuildExportArgs(settings, call), log, log + ".err");
            var result = await childRunner.RunAsync(childCall, ct).ConfigureAwait(false);
            if (result.NotStarted is { } why) return (result, why);
            // The child's own output goes to the run's stdout, then the summary line.
            var summary = $"exit code: {result.ExitCode}; elapsed: {result.ElapsedSeconds.ToString("N0", CultureInfo.InvariantCulture)} s; exporter peak private memory: {result.PeakPrivateMb.ToString("N0", CultureInfo.InvariantCulture)} MB; host free memory lowest {result.MinFreeMb?.ToString("N0", CultureInfo.InvariantCulture) ?? "unknown"} MB; log: {log}";
            output.WriteLine(summary);
            return (result, null);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException)
        {
            return (null, ex.Message);
        }
    }

    private void RecordRun(ScheduleState state, JsonObject block, string family, ChildResult r, string dailyLogPath)
    {
        state.Root["last_run"] = new JsonObject
        {
            ["peak_private_mb"] = r.PeakPrivateMb, ["min_free_mb"] = r.MinFreeMb, ["elapsed_seconds"] = r.ElapsedSeconds,
            ["log_path"] = dailyLogPath, ["family"] = family,
        };
        block["last_run"] = new JsonObject
        {
            ["peak_private_mb"] = r.PeakPrivateMb, ["min_free_mb"] = r.MinFreeMb, ["elapsed_seconds"] = r.ElapsedSeconds,
            ["exit_code"] = r.ExitCode, ["finished_utc"] = ScheduleState.Stamp(clock()),
        };
        state.Root["last_exit_code"] = r.ExitCode;
    }

    /// <summary>The last 500 characters of the failure text: stderr, else stdout, else the not-started or exception message (R15).</summary>
    private static string FailureText(ChildResult? result, string? runError, string what)
    {
        string tail;
        if (result is { Aborted: true })
        {
            tail = $"memory guard stopped the exporter (exit 9): host free memory {result.AbortFreeMb} MB was below the {result.FloorMb} MB floor; the {what} was not recorded as exported";
        }
        else if (runError is not null)
        {
            tail = runError;
        }
        else
        {
            tail = result is null ? "" : File.Exists(result.ErrPath) && new FileInfo(result.ErrPath).Length > 0
                ? File.ReadAllText(result.ErrPath)
                : File.Exists(result.LogPath) ? File.ReadAllText(result.LogPath) : "";
        }
        return tail.Length > 500 ? tail[^500..] : tail;
    }

    // ---- R14: exit 3 reads failed_optional from the OPTIONAL manifest ----

    private void RecordOptionalFailures(ScheduleState state, PlannedCall call, FamilyPlan plan, ChildResult result)
    {
        var failures = ReadFailedOptional(call, plan);
        var list = state.Array("optional_failures");
        foreach (var (table, error) in failures)
        {
            list.Add(new JsonObject
            {
                ["table"] = table, ["window_from"] = call.FromArg, ["window_to"] = call.ToArg,
                ["error"] = error, ["recorded_utc"] = ScheduleState.Stamp(clock()),
            });
        }
        while (list.Count > 200) list.RemoveAt(0);
        output.WriteLine($"optional table(s) failed for {plan.Name} {call.FromArg} .. {call.ToArg}; core export still advances");
    }

    internal IReadOnlyList<(string Table, string Error)> ReadFailedOptional(PlannedCall call, FamilyPlan plan)
    {
        var unknown = new[] { ("unknown", "exit 3; optional manifest not readable") };
        if (string.IsNullOrEmpty(call.Also)) return unknown;
        try
        {
            var also = call.Also.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            var path = OutputPaths.ManifestFilePath(settings.OutDir, settings.Store, call.From, call.To, also, dimensionOrigin: null);
            if (!File.Exists(path)) return unknown;
            var node = JsonNode.Parse(File.ReadAllText(path));
            if (node?["failed_optional"] is not JsonArray arr) return [];
            return arr.Select(f => (f?["table"]?.GetValue<string>() ?? "", f?["error"]?.GetValue<string>() ?? "")).ToArray();
        }
        catch (Exception ex) when (ex is IOException or JsonException or InvalidOperationException or FormatException)
        {
            return unknown;
        }
    }

    private List<string> ReadOptionalTables()
    {
        var names = new List<string>(settings.Families.OptionalTables);
        var file = settings.Families.OptionalTablesFile;
        if (!string.IsNullOrEmpty(file) && File.Exists(file))
        {
            try { names.AddRange(Planner.ParseOptionalTables(File.ReadAllLines(file))); }
            catch (IOException) { }
        }
        return names.Distinct(StringComparer.Ordinal).ToList();
    }

    // ---- R21-R23: the fold and the cleanup ----

    private void FoldDailyLog(string dailyLogPath, DateTime nowUtc, List<CallRecord> calls, PostRunResult? hook, int runExit, double elapsedSeconds)
    {
        try
        {
            var stamp = RunFiles.RunStamp(nowUtc);
            var sb = new StringBuilder();
            sb.Append(CultureInfo.InvariantCulture, $"===== run {stamp} start export {settings.Store}\n");
            foreach (var c in calls)
            {
                sb.Append(RunFiles.SectionText(c.LogPath));
                var err = RunFiles.SectionText(c.LogPath + ".err");
                if (err.Length > 0)
                {
                    sb.Append(CultureInfo.InvariantCulture, $"----- stderr {c.Family}\n");
                    sb.Append(err);
                }
            }
            if (hook is not null)
            {
                sb.Append(CultureInfo.InvariantCulture, $"----- {settings.PostRun!.Name} exit {hook.ExitCode}\n");
                sb.Append(RunFiles.SectionText(hook.LogPath));
            }
            sb.Append(CultureInfo.InvariantCulture, $"===== run {stamp} end exit={runExit} elapsed={elapsedSeconds.ToString("N0", CultureInfo.InvariantCulture)}s\n");
            RunFiles.AppendDaily(dailyLogPath, sb.ToString());
            foreach (var c in calls)
            {
                TryDelete(c.LogPath);
                TryDelete(c.LogPath + ".err");
            }
            if (hook is not null) TryDelete(hook.LogPath);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
    }

    private static void TryDelete(string path)
    {
        try { File.Delete(path); }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException) { }
    }

    private void Cleanup(DateTime nowUtc)
    {
        var r = settings.Retention;
        RunFiles.ClearExpiredLogs(settings.LogDir, nowUtc, clock(), r.LogDays, r.LeftoverHours, settings.PostRun?.Name);
        RunFiles.ClearOldExportOutput(Path.Combine(settings.OutDir, settings.Store), nowUtc, r.OutHours);
        RunFiles.ClearStaleTemp(r.TempDir ?? Path.GetTempPath(), r.TempPrefixes, nowUtc, r.TempHours);
    }

    // ---- plan-only ----

    private static bool IsOtherSchema(string path)
    {
        try
        {
            var text = File.ReadAllText(path).TrimStart('\uFEFF');
            return JsonNode.Parse(text) is JsonObject o && !(o["schema"] is JsonValue v && v.TryGetValue<int>(out var n) && n == ScheduleState.Schema);
        }
        catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    private int PlanOnly(DateTime nowUtc, ScheduleState? loaded, string? stateError)
    {
        var free = memory.FreeMb();
        DateTime? restartUtc = settings.Restart.DelayMinutes > 0 ? restart.RestartUtc() : null;
        var optionalTables = ReadOptionalTables();
        var mode = Planner.ResolveMode(settings, optionalTables);

        string decision;
        var families = new JsonArray();
        var plannedCalls = new JsonArray();
        if (stateError is not null) decision = "refuse_state";
        else if (RestartDelay.IsWithin(nowUtc, restartUtc, settings.Restart.DelayMinutes)) decision = "skip_restart";
        else
        {
            decision = "run";
            var state = loaded ?? ScheduleState.NewCold(settings.Store, settings.Families.QueryStoreEnabled, settings.Schedule.StartUtc, nowUtc);
            if (mode.SkipHourlyFresh) families.Add(new JsonObject { ["name"] = Planner.Fresh, ["status"] = "skipped", ["entries"] = new JsonArray() });
            foreach (var family in Planner.FamilyOrder(mode))
            {
                // fresh_fast with no state block yet starts from the fresh family's last_to, as the real run seeds it (R12).
                var lastTo = family == Planner.FreshFast && !state.Has(Planner.FreshFast) ? state.LastTo(Planner.Fresh) : state.LastTo(family);
                var plan = Planner.PlanFamily(settings, mode, family, lastTo, nowUtc);
                var entries = new JsonArray();
                foreach (var w in plan.Windows)
                {
                    entries.Add(new JsonObject { ["from"] = ScheduleState.Stamp(w.From), ["to"] = ScheduleState.Stamp(w.To), ["gap"] = w.IsGap });
                    if (w.IsGap) continue;
                    var c = Planner.CallFor(mode, plan, w);
                    plannedCalls.Add(new JsonObject
                    {
                        ["family"] = c.Family, ["from"] = c.FromArg, ["to"] = c.ToArg, ["tables"] = c.Tables,
                        ["dimension_origin"] = c.DimensionOrigin, ["also"] = c.Also,
                        ["query_store_lag_hours"] = c.QueryStoreLagHours, ["fresh_guard_minutes"] = c.FreshGuardMinutes,
                        // The child's argument list a real run would pass (the exe left out), so parity also checks the argv.
                        ["argv"] = new JsonArray(Planner.BuildExportArgs(settings, c).Select(a => (JsonNode?)JsonValue.Create(a)).ToArray()),
                    });
                }
                families.Add(new JsonObject { ["name"] = family, ["status"] = plan.Status, ["entries"] = entries });
            }
        }

        var exempt = new JsonArray { Planner.FreshFast };
        if (mode.HasQueryStore && mode.QueryStoreLag == 1) exempt.Add(Planner.QueryStore);
        var optional = new JsonArray();
        foreach (var t in optionalTables) optional.Add(t);
        var record = new JsonObject
        {
            ["kind"] = "plan",
            ["now_utc"] = ScheduleState.Stamp(nowUtc),
            ["store"] = settings.Store,
            ["decision"] = decision,
            ["restart_utc"] = restartUtc is { } r ? ScheduleState.Stamp(r) : null,
            ["free_mb"] = free,
            ["query_store_lag"] = new JsonObject { ["value"] = mode.QueryStoreLag, ["source"] = mode.QueryStoreLagSource },
            ["late_margin"] = new JsonObject { ["value"] = mode.LateMargin, ["source"] = mode.LateSource },
            ["optional_tables"] = optional,
            ["families"] = families,
            ["calls"] = plannedCalls,
            ["cutoff_minute"] = settings.Schedule.CutoffMinute,
            ["exempt_from_cutoff"] = exempt,
            // missing: no state file, so `run` cold-starts. not_schema_2: a readable state file without "schema": 2,
            // which `run` refuses.
            ["state_file"] = loaded is not null ? "loaded" : stateError is null ? "missing" : IsOtherSchema(settings.StatePath) ? "not_schema_2" : "unusable",
        };
        var path = Path.Combine(settings.LogDir, $"plan-only-{settings.Store}-{nowUtc.ToString("yyyyMMdd", CultureInfo.InvariantCulture)}.jsonl");
        RunFiles.AppendDaily(path, record.ToJsonString(new JsonSerializerOptions { Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }) + "\n");
        output.WriteLine($"plan-only: {decision}; wrote {path}");
        return decision == "refuse_state" ? ExitRefused : ExitOk;
    }
}
