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
    return await CliVerbs.CheckConfigAsync(args, Console.Out, Console.Error, CancellationToken.None);
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

return await CliVerbs.ExportAsync(args[1..], Console.Out, Console.Error, CancellationToken.None, stdoutIsConsole: !Console.IsOutputRedirected);

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
