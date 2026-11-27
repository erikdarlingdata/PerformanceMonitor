using System.Globalization;
using System.Text.Json;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>
/// The `run` verb: `darling-export run --config F [--plan-only] [--now ISO]` (the JSON config), or
/// `run --settings F` (a RunSettings JSON, kept for tests). Exactly one of the two. A bad file is exit 2.
/// </summary>
public static class RunCommand
{
    public const string Usage = "Usage: darling-export run (--config <file> | --settings <file>) [--plan-only] [--now <ISO UTC>] [--verbose]";

    /// <summary>The store check `--plan-only` runs: null when the store passed, or one line saying why the check could not run. Throws <see cref="ConfigException"/> for a store the exporter cannot read.</summary>
    public delegate Task<string?> PlanStoreCheck(ExporterConfig config, CancellationToken ct);

    /// <param name="childRunner">Runs each window's child process. The default starts the exporter; a test passes its own.</param>
    /// <param name="planStoreCheck">The store check `--plan-only` runs with a config file. The default is <see cref="StoreCheck.PreflightForPlanAsync"/>.</param>
    public static async Task<int> MainAsync(string[] args, TextWriter output, TextWriter error, CancellationToken ct, IChildRunner? childRunner = null, PlanStoreCheck? planStoreCheck = null)
    {
        string? settingsPath = null, configPath = null;
        var planOnly = false;
        var verbose = false;
        DateTime? now = null;
        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--settings" when i + 1 < args.Length: settingsPath = args[++i]; break;
                case "--config" when i + 1 < args.Length: configPath = args[++i]; break;
                case "--plan-only": planOnly = true; break;
                case "--verbose": verbose = true; break;
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
            // A plan is worked out on any OS, so a dpapi_file password is accepted by plan-only whatever the OS. A real run
            // refuses it off Windows, as `export` does.
            ExporterConfig config;
            try { (settings, config) = RunConfigMapping.LoadWithConfig(configPath, isWindows: OperatingSystem.IsWindows() || planOnly); }
            catch (ConfigException ex)
            {
                error.WriteLine($"Config error: {ex.Message}");
                return RunScheduler.ExitRefused;
            }

            // What every window of this run needs is checked once, before the first window, so a configuration error is
            // exit 2 with one line and no window is started: the store credential and the destination's key variables (a real
            // run only) and the local destination folder, which the upload never creates.
            try
            {
                if (!planOnly)
                {
                    CredentialResolver.Resolve(config.Store.Credential, TextWriter.Null);
                    DestinationCheck.AssertCredentials(config);
                }
                DestinationCheck.Assert(config);
            }
            catch (Exception ex) when (ex is ConfigException or CredentialException)
            {
                error.WriteLine($"Config error: {ex.Message}");
                return RunScheduler.ExitRefused;
            }

            if (planOnly)
            {
                // The plan reads the store's collation when it can, so a store the exporter cannot read is found here and not at
                // the first window. A credential that cannot be resolved or a store that cannot be reached leaves a warning and
                // does not stop the plan.
                try
                {
                    var notChecked = await (planStoreCheck ?? StoreCheck.PreflightForPlanAsync)(config, ct).ConfigureAwait(false);
                    if (notChecked is not null) error.WriteLine($"warning: store collation not checked: {notChecked}");
                }
                catch (ConfigException ex)
                {
                    error.WriteLine($"Config error: {ex.Message}");
                    return RunScheduler.ExitRefused;
                }
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

        if (verbose)
        {
            ExporterLog.Verbose = true;
            settings = settings with { Verbose = true };
        }

        var memory = new SystemHostMemory(error);
        var scheduler = new RunScheduler(
            settings, new RunOptions(planOnly, now), () => DateTime.UtcNow,
            childRunner ?? new ProcessChildRunner(settings.MemoryGuard, memory, output),
            settings.Validate() is null ? RestartDelay.Create(settings.Restart, error) : new NoRestartDetector(),
            memory, output);
        return await scheduler.RunAsync(ct).ConfigureAwait(false);
    }
}
