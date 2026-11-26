using System.Text.Json;
using PerformanceMonitor.Exporter;

if (args.Length > 0 && args[0] == "explain-gate")
{
    return RunExplainGate(args);
}

if (args.Length > 0 && args[0] == "run")
{
    return await PerformanceMonitor.Exporter.Run.RunCommand.MainAsync(args[1..], Console.Out, Console.Error, CancellationToken.None);
}

if (args.Length > 0 && args[0] == "check-config")
{
    return RunCheckConfig(args);
}

if (args.Length > 0 && args[0] == "describe-tables")
{
    return RunDescribeTables(args);
}

if (args.Length > 0 && args[0] is "--help" or "-h" or "help")
{
    Console.Out.Write(Usage.Text());
    return 0;
}

if (args.Length == 0 || args[0] != "export")
{
    Console.Error.Write(Usage.Text());
    return 1;
}

try
{
    var invocation = ExportCommand.Parse(args[1..], DateTime.UtcNow);
    ExporterLog.Verbose = invocation.Verbose;
    DuckDbSession.Configure(invocation.DuckDbMemoryLimit, invocation.DuckDbThreads);
    return await ExportRunner.RunAsync(invocation.Options, Console.Out, CancellationToken.None).ConfigureAwait(false);
}
catch (UsageException ex)
{
    Console.Error.WriteLine(ex.Message);
    return 1;
}
catch (Exception ex) when (ex is ConfigException or CredentialException)
{
    Console.Error.WriteLine($"Config error: {ex.Message}");
    return 2;
}
catch (Exception ex)
{
    // The exception chain (each type and message) goes to stdout, which the scheduler keeps in the run's log, so a failure
    // names the layer that raised it: a bare "The operation has timed out" can come from a connection open, a read or an
    // upload. The stack frames follow only with --verbose. stderr stays one line, because the scheduler records it as the
    // family's last error and the stale alert quotes it, so the line only gains the exception types.
    Console.Out.WriteLine(ExporterLog.DescribeFailure(ex));
    var types = ex.GetType().Name + (ex.InnerException is { } inner ? " <- " + inner.GetType().Name : "");
    Console.Error.WriteLine($"Export failed: {ex.Message} ({types})");
    return 1;
}

// check-config --config F: loads and validates the config, resolves the credential, prints the config with secrets redacted.
static int RunCheckConfig(string[] args)
{
    string? path = null;
    for (var i = 1; i < args.Length; i++)
    {
        if (args[i] == "--config" && i + 1 < args.Length)
        {
            path = args[++i];
        }
        else
        {
            Console.Error.WriteLine($"check-config: unknown argument {args[i]}");
            return 2;
        }
    }

    if (path is null)
    {
        Console.Error.WriteLine("Usage: darling-export check-config --config <file>");
        return 2;
    }

    try
    {
        var config = ConfigLoader.Load(path);
        var secret = CredentialResolver.Resolve(config.Store.Credential);
        if (config.Store.ConnectionStringEnv is not null)
        {
            // Parse it too, so a malformed connection string is caught here and not at the first export.
            StoreConnectionFactory.BuildFromConnectionString(
                ExportOptions.Validate(config.Store.Name, "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", config.Paths.Out, null,
                    config.Store.Host, config.Store.Port, config.Store.Database, config.Store.User, null,
                    config.Store.StatementTimeoutSeconds, false, true, DateTime.UtcNow, applicationName: config.Store.ApplicationName),
                secret);
        }

        Console.Out.WriteLine(ConfigPrinter.Render(config, credentialResolved: true));
        return 0;
    }
    catch (Exception ex) when (ex is ConfigException or CredentialException)
    {
        Console.Error.WriteLine($"Config error: {ex.Message}");
        return 2;
    }
}

// describe-tables [--json]: kind, time column, keys and grain for every table. No store connection.
static int RunDescribeTables(string[] args)
{
    var json = false;
    for (var i = 1; i < args.Length; i++)
    {
        if (args[i] == "--json")
        {
            json = true;
        }
        else
        {
            Console.Error.WriteLine($"describe-tables: unknown argument {args[i]}");
            return 2;
        }
    }

    var rows = TableCatalog.Build();
    Console.Out.Write(json ? TableCatalog.RenderJson(rows) + Environment.NewLine : TableCatalog.RenderText(rows));
    return 0;
}

// explain-gate --tables a,b --from <ISO> --to <ISO> [--store s]: prints the optional-table gate SQL. No store connection.
static int RunExplainGate(string[] args)
{
    string? tables = null, from = null, to = null, store = "unknown";
    for (var i = 1; i < args.Length; i++)
    {
        string Next() => i + 1 < args.Length ? args[++i] : throw new ArgumentException($"{args[i]} needs a value");
        switch (args[i])
        {
            case "--tables": tables = Next(); break;
            case "--from": from = Next(); break;
            case "--to": to = Next(); break;
            case "--store": store = Next(); break;
            default:
                Console.Error.WriteLine($"explain-gate: unknown argument {args[i]}");
                return 2;
        }
    }

    if (string.IsNullOrWhiteSpace(tables) || from is null || to is null)
    {
        Console.Error.WriteLine("Usage: darling-export explain-gate --tables a,b,c --from <ISO UTC> --to <ISO UTC> [--store s]");
        return 2;
    }

    var chosen = new List<OptionalTable>();
    foreach (var name in tables.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
        var entry = OptionalTables.All.FirstOrDefault(t => t.Name == name);
        if (entry is null)
        {
            Console.Error.WriteLine($"explain-gate: unknown optional table '{name}'");
            return 2;
        }

        chosen.Add(entry);
    }

    if (!DateTime.TryParse(from, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal, out var f)
        || !DateTime.TryParse(to, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal, out var t))
    {
        Console.Error.WriteLine("explain-gate: --from/--to must be ISO timestamps");
        return 2;
    }

    Console.Out.Write(OptionalGateScript.Build(chosen, f, t, store));
    return 0;
}
