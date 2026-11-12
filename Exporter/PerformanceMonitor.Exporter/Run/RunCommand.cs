using System.Globalization;
using System.Text.Json;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>
/// The `run` verb: `darling-export run --config F [--plan-only] [--now ISO]` (the JSON config), or
/// `run --settings F` (a RunSettings JSON, kept for tests). Exactly one of the two. A bad file is exit 2.
/// </summary>
public static class RunCommand
{
    public const string Usage = "Usage: darling-export run (--config <file> | --settings <file>) [--plan-only] [--now <ISO UTC>]";

    public static async Task<int> MainAsync(string[] args, TextWriter output, TextWriter error, CancellationToken ct)
    {
        string? settingsPath = null, configPath = null;
        var planOnly = false;
        DateTime? now = null;
        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--settings" when i + 1 < args.Length: settingsPath = args[++i]; break;
                case "--config" when i + 1 < args.Length: configPath = args[++i]; break;
                case "--plan-only": planOnly = true; break;
                case "--now" when i + 1 < args.Length:
                    if (!DateTime.TryParse(args[++i], CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
                    {
                        error.WriteLine($"--now is not a date: {args[i]}");
                        return RunScheduler.ExitRefused;
                    }
                    now = parsed;
                    break;
                default:
                    error.WriteLine($"unknown or incomplete argument: {args[i]}");
                    error.WriteLine(Usage);
                    return RunScheduler.ExitRefused;
            }
        }
        if ((settingsPath is null) == (configPath is null))
        {
            error.WriteLine(Usage);
            return RunScheduler.ExitRefused;
        }

        RunSettings settings;
        if (configPath is not null)
        {
            // Plan-only never resolves the credential or opens the store, so a dpapi_file password is accepted on any OS
            // (a config can be planned on any OS). A real run refuses it off Windows, as `export` does.
            try { settings = RunConfigMapping.Load(configPath, isWindows: OperatingSystem.IsWindows() || planOnly); }
            catch (ConfigException ex)
            {
                error.WriteLine($"Config error: {ex.Message}");
                return RunScheduler.ExitRefused;
            }
        }
        else
        {
            try { settings = RunSettings.FromJson(File.ReadAllText(settingsPath!)); }
            catch (Exception ex) when (ex is IOException or JsonException or UnauthorizedAccessException)
            {
                error.WriteLine($"cannot read settings {settingsPath}: {ex.Message}");
                return RunScheduler.ExitRefused;
            }
        }

        var memory = new SystemHostMemory(error);
        var scheduler = new RunScheduler(
            settings, new RunOptions(planOnly, now), () => DateTime.UtcNow,
            new ProcessChildRunner(settings.MemoryGuard, memory, output),
            settings.Validate() is null ? RestartDelay.Create(settings.Restart, error) : new NoRestartDetector(),
            memory, output);
        return await scheduler.RunAsync(ct).ConfigureAwait(false);
    }
}
