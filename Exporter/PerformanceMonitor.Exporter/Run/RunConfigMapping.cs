using System.Globalization;
using System.Text.Json;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>
/// Maps the JSON config (<see cref="ExporterConfig"/>) to the <see cref="RunSettings"/> the `run` verb
/// executes. One function, so `run --config F` and `run --settings F` share every rule after this point.
/// </summary>
public static class RunConfigMapping
{
    /// <summary>
    /// The settings for <paramref name="config"/>, loaded from <paramref name="configPath"/>. Throws a
    /// <see cref="ConfigException"/> (exit 2) for a config the scheduler cannot run yet.
    /// <para>The child call. When the config says only what today's `export` flags can say (a dpapi_file password, the
    /// default host, port and database, and any application_name), the child is the flags-only
    /// `export ...` call, with the application name always, and the login pair only when the login is not the default
    /// darling one. Otherwise the child is `export --config F ...`: the config then carries the login, the host and the
    /// application name, and the per-call flags are the same.</para>
    /// </summary>
    public static RunSettings ToRunSettings(ExporterConfig config, string configPath)
    {
        if (config.Schedule.HourlyStartMinute != 20)
        {
            throw new ConfigException($"schedule.hourly_start_minute: run supports only 20 for now, got {config.Schedule.HourlyStartMinute}");
        }

        var (upload, region) = UploadFor(config);
        var store = config.Store;
        var flagsOnly = store.ConnectionStringEnv is null
            && store.Password is { Source: CredentialSources.DpapiFile }
            && store.Host == "127.0.0.1" && store.Port == 5641 && store.Database == "darling";

        string? pgUser = null, pgCredentialFile = null, applicationName = null;
        List<string> baseArgs = ["export"];
        if (flagsOnly)
        {
            applicationName = store.ApplicationName;
            var credentialFile = store.Password!.Path!;
            if (store.User != "darling" || !string.Equals(credentialFile, ExportOptions.DefaultCredentialFile, StringComparison.OrdinalIgnoreCase))
            {
                pgUser = store.User;
                pgCredentialFile = credentialFile;
            }
        }
        else
        {
            baseArgs = ["export", "--config", Path.GetFullPath(configPath)];
        }

        DateTime? startUtc = null;
        if (config.Schedule.StartUtc is { } start)
        {
            startUtc = DateTime.Parse(start, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        }

        var families = config.Families;
        return new RunSettings
        {
            Store = store.Name,
            OutDir = config.Paths.Out,
            StateDir = config.Paths.State,
            LogDir = config.Paths.Logs,
            Upload = upload,
            ChildExe = null,
            ChildBaseArgs = baseArgs,
            PgUser = pgUser,
            PgCredentialFile = pgCredentialFile,
            // The exporter's own defaults (shadow, raw) pass no flag, as an absent config\*.txt file does today.
            ProcedurePlanReuse = config.Export.ProcedurePlanReuse == "shadow" ? null : config.Export.ProcedurePlanReuse,
            QueryStoreSource = !families.QueryStore.Enabled || families.QueryStore.Source == "raw" ? null : families.QueryStore.Source,
            // Every call names the state folder. The region is named only when the destination sets one.
            ExportStateDir = config.Paths.State,
            AwsRegion = region,
            ApplicationName = applicationName,
            StatementTimeoutSeconds = store.StatementTimeoutSeconds,
            DuckdbMemoryLimit = config.Export.DuckdbMemoryLimit,
            DuckdbThreads = config.Export.DuckdbThreads,
            Families = new FamilySettings
            {
                FreshTables = families.Fresh.Tables?.ToList(),
                QueryStoreEnabled = families.QueryStore.Enabled,
                OptionalTables = families.Fresh.OptionalTables.ToList(),
                OptionalTablesFile = families.Fresh.OptionalTablesFile,
                // The defaults (3 and 15) map to null, which the plan reports as source "default".
                QueryStoreLagHours = families.QueryStore.LagHours == Planner.DefaultQueryStoreLag ? null : families.QueryStore.LagHours,
                FastWindowMinutes = families.Fast.WindowMinutes,
                LateRowMarginMinutes = families.Fast.LateRowMarginMinutes == Planner.DefaultLateMargin ? null : families.Fast.LateRowMarginMinutes,
            },
            Schedule = new ScheduleSettings
            {
                MaxWindowsPerFamily = config.Schedule.MaxWindowsPerFamily,
                MaxWindowAgeHours = config.Schedule.MaxWindowAgeHours,
                CutoffMinute = config.Schedule.CutoffMinute,
                StartUtc = startUtc,
            },
            MemoryGuard = new MemoryGuardSettings
            {
                Enabled = config.MemoryGuard.Enabled,
                MinFreeMb = config.MemoryGuard.MinFreeMb,
                StartMultiplier = config.MemoryGuard.StartMultiplier,
            },
            Restart = new RestartSettings
            {
                DelayMinutes = config.RestartDelay.Minutes,
                Detector = config.RestartDelay.Detector,
                DarlingServiceName = config.RestartDelay.DarlingServiceName,
                DarlingRuntimePath = config.RestartDelay.DarlingRuntimePath,
                File = config.RestartDelay.File,
                Command = config.RestartDelay.Command?.ToList(),
            },
            Retention = new RetentionSettings
            {
                LogDays = config.Retention.LogDays,
                LeftoverHours = config.Retention.LeftoverHours,
                OutHours = config.Retention.OutHours,
                TempHours = config.Retention.TempHours,
            },
            PostRun = config.PostRun is { } hook
                ? new PostRunSettings { Name = hook.Name, Command = hook.Command.ToList(), TimeoutSeconds = hook.TimeoutSeconds }
                : null,
        };
    }

    /// <summary>
    /// The child's `--upload` value and `--aws-region` from `destination`. s3 is `s3://bucket[/prefix]` with the
    /// destination's region, or none when unset (the AWS SDK then resolves it from its own chain); local is the folder
    /// (joined with its prefix). No destination is the default, the local folder next to paths.out.
    /// A destination `--upload` cannot express (azure_blob, an S3 endpoint, credentials, checksum or hive layout) is refused.
    /// </summary>
    internal static (string Upload, string? Region) UploadFor(ExporterConfig config)
    {
        if (config.Destination is not { } d)
        {
            var parent = Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(config.Paths.Out)) ?? config.Paths.Out;
            return (Path.Combine(parent, "landing"), null);
        }

        Upload.UploadSettings parsed;
        try { parsed = Upload.UploadSettings.FromJson(d); }
        catch (ArgumentException ex) { throw new ConfigException(ex.Message); }

        switch (parsed)
        {
            case Upload.UploadSettings.S3 s3:
                if (s3.EndpointUrl is not null || s3.ForcePathStyle || s3.Checksum != Upload.S3Checksum.Sha256
                    || s3.Credentials is not (null or Upload.S3Credentials.Default) || s3.HiveStoreFolder)
                {
                    throw new ConfigException("destination: run supports only bucket, prefix and region for s3 for now");
                }
                var prefix = s3.Prefix.Trim('/');
                return (prefix.Length == 0 ? $"s3://{s3.Bucket}" : $"s3://{s3.Bucket}/{prefix}", string.IsNullOrWhiteSpace(s3.Region) ? null : s3.Region);
            case Upload.UploadSettings.Local local:
                if (local.HiveStoreFolder) throw new ConfigException("destination.hive_store_folder: run does not support it for now");
                var path = local.Prefix.Length == 0 ? local.Path : Path.Combine(local.Path, local.Prefix.Replace('/', Path.DirectorySeparatorChar));
                if (string.IsNullOrWhiteSpace(path)) throw new ConfigException("destination.path: must not be empty");
                return (path, null);
            default:
                throw new ConfigException($"destination.type: run supports s3 and local for now, got '{d.GetProperty("type").GetString()}'");
        }
    }

    /// <summary>Loads and maps a config file. Every failure is a <see cref="ConfigException"/>.</summary>
    public static RunSettings Load(string configPath, bool? isWindows = null)
    {
        try
        {
            return ToRunSettings(ConfigLoader.Load(configPath, isWindows), configPath);
        }
        catch (JsonException ex)
        {
            throw new ConfigException($"config: {ex.Message}");
        }
    }
}
