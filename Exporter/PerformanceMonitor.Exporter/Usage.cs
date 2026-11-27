using System.Text;

namespace PerformanceMonitor.Exporter;

/// <summary>One flag of the `export` verb: its name, the value it takes (null for a switch) and what it does.</summary>
public sealed record UsageFlag(string Name, string? Value, string Description);

/// <summary>The usage text `darling-export` prints with no verb or an unknown one, and for `--help`.</summary>
public static class Usage
{
    /// <summary>Every flag `export` accepts, in the order the usage text lists them.</summary>
    public static readonly IReadOnlyList<UsageFlag> ExportFlags =
    [
        new("--config", "<file>", "Read defaults from a JSON config. Any flag given overrides it."),
        new("--store", "<name>", "The store name: lower case, starts with a letter, at most 31 characters."),
        new("--from", "<ISO UTC>", "Window start."),
        new("--to", "<ISO UTC>", "Window end."),
        new("--out", "<dir>", "Folder the Parquet files and the manifest are written to."),
        new("--tables", "<a,b,c>", "Core and optional tables to export. Default: the core tables."),
        new("--also", "<a,b,c>", "Optional tables to export in addition to the core tables."),
        new("--dimension-origin", "<name>", "Which family writes the dimension tables."),
        new("--pg-host", "<host>", "Store host."),
        new("--pg-port", "<port>", "Store port."),
        new("--pg-database", "<name>", "Store database."),
        new("--pg-user", "<name>", "Store login."),
        new("--pg-credential-file", "<file>", "File that holds the store password. Not allowed with --config."),
        new("--statement-timeout-seconds", "<n>", "Per-statement timeout for the store sessions."),
        new("--application-name", "<name>", "PostgreSQL application_name of the exporter's sessions."),
        new("--upload", "<destination>", "Upload the files and the manifest. For example s3://bucket/prefix or a folder path."),
        new("--aws-region", "<region>", "Region for an S3 --upload."),
        new("--upload-destination", null, "Upload to the destination in the --config file. Use it in place of --upload."),
        new("--state-dir", "<dir>", "Folder for the exporter's state files."),
        new("--ignore-state", null, "Write every dimension key again, as if the state files were empty."),
        new("--dry-run", null, "Print what the export would read and write. Nothing is read from the store or written."),
        new("--allow-open-window", null, "Export a window whose end is too recent to be complete."),
        new("--allow-compressed", null, "Manual catch-up only: skip the compressed-chunk guard's refusal and double the content-fetch pacer's sleeps. The scheduler never passes this flag."),
        new("--duckdb-memory-limit", "<size>", "DuckDB memory limit, for example 4GB."),
        new("--duckdb-threads", "<n>", "DuckDB thread count."),
        new("--pg-text-fetch-all", null, "Fetch the text of every statement, not only new ones."),
        new("--plan-fetch-all", null, "Turn the plan-fetch gate off, so every new plan key has its XML fetched."),
        new("--procedure-plan-reuse", "<off|shadow|on>", "Whether procedure_plan skips plan XML for a digest fetched less than 6 hours ago."),
        new("--query-store-source", "<raw|wide|compare>", "Where the Query Store window is read from."),
        new("--query-store-lag-hours", "<1|2|3>", "How many hours behind the Query Store window runs."),
        new("--fresh-guard-minutes", "<2-15>", "Margin before now that a fresh window must stay behind."),
        new("--verbose", null, "Also print the session-guard lines, and on an interactive console the full stack trace of a failure."),
    ];

    /// <summary>The usage text: the verbs, then every `export` flag.</summary>
    public static string Text()
    {
        var sb = new StringBuilder();
        sb.AppendLine("Usage: darling-export export [--config <file>] --store <name> --from <ISO UTC> --to <ISO UTC> --out <dir> [options]");
        sb.AppendLine("       darling-export run (--config <file> | --settings <file>) [--plan-only] [--now <ISO UTC>] [--verbose]");
        sb.AppendLine("       darling-export check-config --config <file>");
        sb.AppendLine("       darling-export describe-tables [--json]");
        sb.AppendLine("       darling-export explain-gate --tables <a,b,c> --from <ISO UTC> --to <ISO UTC> [--store <name>]");
        sb.AppendLine();
        sb.AppendLine("run options:");
        sb.AppendLine("  --plan-only         Print and log the windows a run would export. Does not open the store or upload.");
        sb.AppendLine("  --now <ISO UTC>     Set the clock for a plan.");
        sb.AppendLine("  --verbose           Pass --verbose to every export the run starts.");
        sb.AppendLine();
        sb.AppendLine("export options:");
        foreach (var flag in ExportFlags)
        {
            var left = flag.Value is null ? flag.Name : $"{flag.Name} {flag.Value}";
            sb.AppendLine($"  {left,-36}{flag.Description}");
        }

        return sb.ToString();
    }
}
