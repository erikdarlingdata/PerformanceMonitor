using System.Globalization;
using System.Text.Json.Nodes;
using PerformanceMonitor.Exporter.Run;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>A temp root with the three folders `run` uses, plus helpers to seed a state file and read it back.</summary>
public sealed class RunRoot : IDisposable
{
    public string Root { get; } = Path.Combine(Path.GetTempPath(), "darling-export-runtest-" + Guid.NewGuid().ToString("N"));
    public string Out => Path.Combine(Root, "out");
    public string State => Path.Combine(Root, "state");
    public string Logs => Path.Combine(Root, "logs");
    public string Temp => Path.Combine(Root, "tmp");
    public string StatePath => Path.Combine(State, "export-schedule.json");

    public RunRoot() { Directory.CreateDirectory(Root); Directory.CreateDirectory(Temp); }

    public RunSettings Settings(string store = "testh", Func<RunSettings, RunSettings>? tweak = null)
    {
        var s = new RunSettings
        {
            Store = store, OutDir = Out, StateDir = State, LogDir = Logs, Upload = "s3://example-bucket/landing",
            ChildExe = "child-exe", ChildBaseArgs = ["export"],
            Schedule = new ScheduleSettings { CutoffMinute = 60 },
            MemoryGuard = new MemoryGuardSettings { MinFreeMb = 1 },
            Restart = new RestartSettings { DelayMinutes = 0 },
            Retention = new RetentionSettings { TempDir = Temp },
        };
        return tweak is null ? s : tweak(s);
    }

    /// <summary>A typical state: schema 2, both families at 12:00Z, plus keys this code does not know.</summary>
    public JsonObject SeedState(string store = "testh", bool queryStore = true, Action<JsonObject>? tweak = null)
    {
        var lt = Utc(2026, 9, 28, 12, 0).ToString("o", CultureInfo.InvariantCulture);
        JsonObject Fam() => new() { ["last_to"] = lt, ["last_success_utc"] = null, ["last_failure_utc"] = null, ["last_error"] = null };
        var o = new JsonObject
        {
            ["schema"] = 2, ["store"] = store, ["fresh"] = Fam(),
        };
        if (queryStore) o["query_store"] = Fam();
        o["gaps"] = new JsonArray();
        o["last_attempt_utc"] = null; o["last_exit_code"] = null; o["last_run"] = null; o["exporter_version"] = null;
        o["last_exported_day"] = "2026-09-01"; o["cutover_utc"] = lt;
        tweak?.Invoke(o);
        Directory.CreateDirectory(State);
        File.WriteAllText(StatePath, o.ToJsonString(new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));
        return o;
    }

    public JsonObject ReadState() => (JsonObject)JsonNode.Parse(File.ReadAllText(StatePath))!;

    public static DateTime Utc(int y, int mo, int d, int h, int mi, int s = 0) => new(y, mo, d, h, mi, s, DateTimeKind.Utc);

    public void Dispose()
    {
        try { Directory.Delete(Root, recursive: true); } catch (IOException) { }
    }
}

/// <summary>Records every call and answers with a scripted result. The default child prints "stub ran" and exits 0.</summary>
public sealed class FakeChild : IChildRunner
{
    public List<ChildCall> Calls { get; } = [];
    public Func<ChildCall, int> ExitFor { get; set; } = _ => 0;
    public Func<ChildCall, string?> StderrFor { get; set; } = _ => null;
    public Action<ChildCall>? OnCall { get; set; }
    public Func<ChildCall, ChildResult?>? Override { get; set; }

    public Task<ChildResult> RunAsync(ChildCall call, CancellationToken ct)
    {
        Calls.Add(call);
        OnCall?.Invoke(call);
        if (Override?.Invoke(call) is { } custom)
        {
            File.WriteAllText(call.LogPath, "stub ran\n");
            return Task.FromResult(custom with { LogPath = call.LogPath, ErrPath = call.ErrPath });
        }
        var code = ExitFor(call);
        File.WriteAllText(call.LogPath, "stub ran\n");
        if (StderrFor(call) is { } err) File.WriteAllText(call.ErrPath, err);
        return Task.FromResult(new ChildResult
        {
            ExitCode = code, PeakPrivateMb = 321, MinFreeMb = 20000, ElapsedSeconds = 1.5, FloorMb = 1,
            LogPath = call.LogPath, ErrPath = call.ErrPath,
        });
    }

    public static string Arg(ChildCall c, string flag)
    {
        var i = c.Args.ToList().IndexOf(flag);
        return i >= 0 && i + 1 < c.Args.Count ? c.Args[i + 1] : "";
    }

    public static bool Has(ChildCall c, string flag) => c.Args.Contains(flag);
    public static string Family(ChildCall c) =>
        Arg(c, "--dimension-origin") switch { "query_stats" => "fresh", "query_store" => "query_store", _ => "fresh_fast" };
}

/// <summary>A throwing runner: proves a code path never starts a child.</summary>
public sealed class NoChildAllowed : IChildRunner
{
    public Task<ChildResult> RunAsync(ChildCall call, CancellationToken ct) => throw new InvalidOperationException("a child was started");
}

public sealed class FixedRestart(DateTime? at) : IRestartDetector
{
    public int Reads { get; private set; }
    public DateTime? RestartUtc() { Reads++; return at; }
}

public static class Runs
{
    public static readonly DateTime Pin = RunRoot.Utc(2026, 9, 28, 16, 30);

    public static async Task<(int Exit, string Output)> Go(
        RunSettings settings, IChildRunner child, DateTime now, DateTime? clockNow = null, IRestartDetector? restart = null,
        bool planOnly = false, IHostMemory? memory = null)
    {
        var output = new StringWriter { NewLine = "\n" };
        var real = clockNow ?? now;
        var sched = new RunScheduler(settings, new RunOptions(planOnly, now), () => real, child,
            restart ?? new NoRestartDetector(), memory ?? new FakeHostMemory(32768), output);
        var exit = await sched.RunAsync(CancellationToken.None);
        return (exit, output.ToString());
    }
}
