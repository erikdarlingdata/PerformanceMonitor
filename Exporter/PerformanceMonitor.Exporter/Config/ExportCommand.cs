namespace PerformanceMonitor.Exporter;

/// <summary>A bad command line: an unknown flag, a flag with no value, or a value that is not a number. The CLI prints the message and exits 1.</summary>
public sealed class UsageException(string message) : ArgumentException(message);

/// <summary>The parsed `export` command: the options plus the two DuckDB limits the process applies before it runs.</summary>
public sealed record ExportInvocation(ExportOptions Options, string DuckDbMemoryLimit, int DuckDbThreads);

/// <summary>
/// Parses `export [--config F] [flags]`. With no config, every flag and default is the one `export` has always had
/// (the boxes call it that way). With `--config`, the config supplies the defaults and any flag given overrides it.
/// </summary>
public static class ExportCommand
{
    public static ExportInvocation Parse(string[] args, DateTime nowUtc, ExporterConfig? preloaded = null)
    {
        string? configPath = null;
        string? store = null, from = null, to = null, outDir = null, tables = null, pgCredentialFile = null;
        string? pgHost = null, pgDatabase = null, pgUser = null, upload = null, awsRegion = null, stateDir = null;
        string? applicationName = null, duckDbMemoryLimit = null, dimensionOrigin = null, also = null, procedurePlanReuse = null, queryStoreSource = null;
        int? pgPort = null, statementTimeoutSeconds = null, duckDbThreads = null, queryStoreLagHours = null, freshGuardMinutes = null;
        bool dryRun = false, allowOpenWindow = false, ignoreState = false, allowCompressed = false, pgTextFetchAll = false, planFetchAll = false;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            string Next() => i + 1 < args.Length ? args[++i] : throw new UsageException($"{arg} needs a value");
            int NextInt() => int.TryParse(Next(), out var n) ? n : throw new UsageException($"{arg} needs an integer");

            switch (arg)
            {
                case "--config": configPath = Next(); break;
                case "--store": store = Next(); break;
                case "--from": from = Next(); break;
                case "--to": to = Next(); break;
                case "--out": outDir = Next(); break;
                case "--tables": tables = Next(); break;
                case "--pg-host": pgHost = Next(); break;
                case "--pg-port": pgPort = NextInt(); break;
                case "--pg-database": pgDatabase = Next(); break;
                case "--pg-user": pgUser = Next(); break;
                case "--pg-credential-file": pgCredentialFile = Next(); break;
                case "--statement-timeout-seconds": statementTimeoutSeconds = NextInt(); break;
                case "--dry-run": dryRun = true; break;
                case "--allow-open-window": allowOpenWindow = true; break;
                case "--upload": upload = Next(); break;
                case "--aws-region": awsRegion = Next(); break;
                case "--state-dir": stateDir = Next(); break;
                case "--application-name": applicationName = Next(); break;
                case "--ignore-state": ignoreState = true; break;
                case "--duckdb-memory-limit": duckDbMemoryLimit = Next(); break;
                case "--duckdb-threads": duckDbThreads = NextInt(); break;
                case "--dimension-origin": dimensionOrigin = Next(); break;
                case "--also": also = Next(); break;
                case "--allow-compressed": allowCompressed = true; break;
                case "--pg-text-fetch-all": pgTextFetchAll = true; break;
                case "--plan-fetch-all": planFetchAll = true; break;
                case "--procedure-plan-reuse": procedurePlanReuse = Next(); break;
                case "--query-store-source": queryStoreSource = Next(); break;
                // An unparseable value becomes -1, which Validate rejects with the flag's own message.
                case "--fresh-guard-minutes": freshGuardMinutes = int.TryParse(Next(), out var guard) ? guard : -1; break;
                case "--query-store-lag-hours": queryStoreLagHours = int.TryParse(Next(), out var lag) ? lag : -1; break;
                default: throw new UsageException($"Unknown argument: {arg}");
            }
        }

        var config = preloaded ?? (configPath is null ? null : ConfigLoader.Load(configPath));
        CredentialSpec? credential = null;

        if (config is not null)
        {
            if (pgCredentialFile is not null)
            {
                throw new ConfigException("--pg-credential-file: not allowed with --config; set store.password in the config");
            }

            if (config.Store.ConnectionStringEnv is not null
                && (pgHost is not null || pgPort is not null || pgDatabase is not null || pgUser is not null))
            {
                throw new ConfigException("--pg-host/--pg-port/--pg-database/--pg-user: not allowed when store.connection_string_env is set");
            }

            store ??= config.Store.Name;
            outDir ??= config.Paths.Out;
            stateDir ??= config.Paths.State;
            pgHost ??= config.Store.Host;
            pgPort ??= config.Store.Port;
            pgDatabase ??= config.Store.Database;
            pgUser ??= config.Store.User;
            statementTimeoutSeconds ??= config.Store.StatementTimeoutSeconds;
            duckDbMemoryLimit ??= config.Export.DuckdbMemoryLimit;
            duckDbThreads ??= config.Export.DuckdbThreads;
            procedurePlanReuse ??= config.Export.ProcedurePlanReuse;
            queryStoreSource ??= config.Families.QueryStore.Source;
            queryStoreLagHours ??= config.Families.QueryStore.LagHours;
            freshGuardMinutes ??= config.Families.Fast.LateRowMarginMinutes;
            applicationName ??= config.Store.ApplicationName;
            credential = config.Store.Credential;
        }

        var options = ExportOptions.Validate(
            store, from, to, outDir, tables,
            pgHost ?? "127.0.0.1", pgPort ?? 5641, pgDatabase ?? "darling", pgUser ?? "darling", pgCredentialFile,
            statementTimeoutSeconds ?? 600, dryRun, allowOpenWindow, nowUtc,
            upload, awsRegion, stateDir, ignoreState, dimensionOrigin, also, allowCompressed, pgTextFetchAll,
            planFetchAll, procedurePlanReuse, queryStoreSource, queryStoreLagHours, freshGuardMinutes,
            applicationName, credential);

        if (string.IsNullOrWhiteSpace(stateDir))
        {
            throw new ConfigException("--state-dir is required (or set paths.state in a --config file)");
        }

        return new ExportInvocation(options, duckDbMemoryLimit ?? DuckDbSession.DefaultMemoryLimit, duckDbThreads ?? DuckDbSession.DefaultThreads);
    }
}
