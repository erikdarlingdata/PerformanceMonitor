/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingCliCommands
{
    /// <summary>What <c>--export-investigations</c> was asked for (#6236).</summary>
    internal sealed record ExportInvestigationsArgs(
        string FilePath, DateTime? SinceUtc, DateTime? UntilUtc, string? ServerName, string? Verdict, bool Force, string? ConfigPath);

    /// <summary>Parses <c>--export-investigations &lt;file&gt; [--since d] [--until d] [--server name] [--verdict v] [--force] [--config path]</c>.
    /// A date is ISO 8601 (a date alone is its midnight UTC); one with no offset is UTC.</summary>
    internal static bool TryParseExportInvestigationsArgs(string[] rest, out ExportInvestigationsArgs? parsed, out string? errorMessage)
    {
        parsed = null;
        errorMessage = null;
        string? file = null, server = null, verdict = null, configPath = null;
        DateTime? since = null, until = null;
        var force = false;
        for (var i = 0; i < rest.Length; i++)
        {
            var arg = rest[i];
            if (string.Equals(arg, "--force", StringComparison.OrdinalIgnoreCase))
            {
                force = true;
                continue;
            }

            var takesValue = arg.ToLowerInvariant() is "--since" or "--until" or "--server" or "--verdict" or "--config";
            if (!takesValue)
            {
                if (arg.StartsWith("--", StringComparison.Ordinal))
                {
                    errorMessage = $"Unknown option {arg}.";
                    return false;
                }

                if (file is not null)
                {
                    errorMessage = $"--export-investigations takes ONE file; got '{file}' and '{arg}'.";
                    return false;
                }

                file = arg;
                continue;
            }

            if (i + 1 >= rest.Length || rest[i + 1].StartsWith("--", StringComparison.Ordinal))
            {
                errorMessage = $"{arg} needs a value.";
                return false;
            }

            var value = rest[++i];
            switch (arg.ToLowerInvariant())
            {
                case "--server":
                    server = value;
                    break;
                case "--verdict":
                    verdict = value;
                    break;
                case "--config":
                    configPath = value;
                    break;
                default:
                    if (!DateTimeOffset.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var when))
                    {
                        errorMessage = $"{arg} '{value}' is not a date. Use 2026-10-01 or 2026-10-01T06:00:00Z.";
                        return false;
                    }

                    if (string.Equals(arg, "--since", StringComparison.OrdinalIgnoreCase))
                        since = when.UtcDateTime;
                    else
                        until = when.UtcDateTime;
                    break;
            }
        }

        if (string.IsNullOrWhiteSpace(file))
        {
            errorMessage = "--export-investigations needs the file to write.";
            return false;
        }

        if (InvestigationFeedbackExport.VerdictRefusal(verdict) is { } refusal)
        {
            errorMessage = refusal;
            return false;
        }

        if (since is { } from && until is { } to && from >= to)
        {
            errorMessage = "--since must be before --until.";
            return false;
        }

        parsed = new ExportInvestigationsArgs(file, since, until, server, string.IsNullOrWhiteSpace(verdict) ? null : verdict.Trim(), force, configPath);
        return true;
    }

    /// <summary>
    /// <c>--export-investigations &lt;file&gt;</c> (#6236): writes every investigation case that has feedback, one JSON object per line, to
    /// the file: the alert, the brief and evidence the agent was given, the report it wrote and the verdict and note a person recorded.
    /// Collected text is masked at <c>mcp.dataDetail</c>, as <c>get_investigation_reports</c> masks it. Exit codes are
    /// <see cref="CollectorToggleExitCode"/>'s: 1 for a usage, configuration or file problem, 2 when the store cannot be read.
    /// </summary>
    public static async Task<int> ExportInvestigationsAsync(string[] rest, TextWriter output, TextWriter error, CancellationToken cancellationToken)
    {
        const string Verb = "--export-investigations";
        if (!TryParseExportInvestigationsArgs(rest, out var args, out var parseError))
        {
            error.WriteLine(parseError);
            output.WriteLine($"Usage: {Verb} <file> [--since <date>] [--until <date>] [--server <name>] [--verdict right|partly|wrong] [--force] [--config <path to darling.json>]");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        if (File.Exists(args!.FilePath) && !args.Force)
        {
            error.WriteLine($"'{args.FilePath}' already exists. Pick another file, or add --force to replace it.");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        var mode = McpDataDetail.Full;
        try
        {
            mode = ReadConfiguredDataDetail(args.ConfigPath);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not load configuration: {ex.Message}");
            return CollectorToggleExitCode.UsageOrConfig;
        }

        var created = TryCreateCollectorStoreDataSource(Verb, args.ConfigPath, error);
        if (created is null)
        {
            return CollectorToggleExitCode.UsageOrConfig;
        }

        await using var dataSource = created;
        return await ExportInvestigationsAsync(dataSource, args, mode, output, error, cancellationToken);
    }

    /// <summary>The export on an open store, so a live test drives exactly what the command line does.</summary>
    internal static async Task<int> ExportInvestigationsAsync(
        NpgsqlDataSource dataSource, ExportInvestigationsArgs args, McpDataDetail mode, TextWriter output, TextWriter error,
        CancellationToken cancellationToken)
    {
        var scope = await ResolveCollectorScopeAsync(dataSource, args.ServerName, error);
        if (scope.FailureExitCode is { } failed)
        {
            return failed;
        }

        try
        {
            var owner = new StoreOwnerSource();
            owner.Publish(dataSource);
            using var keys = new PseudonymKeySource(owner);
            var key = await PseudonymKeySources.KeyForAsync(keys, mode, cancellationToken);
            var store = new InvestigationRequestStore(dataSource, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);
            var (lines, truncated) = await InvestigationFeedbackExport.ReadLinesAsync(
                store, args.SinceUtc, args.UntilUtc, scope.ServerId, args.Verdict, mode, key, cancellationToken);

            /* Write beside the target and move it in, so a failure never leaves half a file where an earlier export was. */
            var temp = args.FilePath + ".partial";
            await File.WriteAllTextAsync(temp, string.Concat(lines.Select(l => l + "\n")), new UTF8Encoding(false), cancellationToken);
            File.Move(temp, args.FilePath, overwrite: true);
            output.WriteLine($"Wrote {lines.Count} investigation case(s) with feedback to {args.FilePath} (one JSON object per line, masked at {McpDataDetailModes.Word(mode)}).");
            if (truncated)
            {
                output.WriteLine($"The filter held more than {InvestigationRequestStore.FeedbackExportLimit} cases; these are the newest. Narrow it with --since, --until, --server or --verdict for the rest.");
            }

            return CollectorToggleExitCode.Success;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            error.WriteLine($"Could not write '{args.FilePath}': {ex.Message}");
            return CollectorToggleExitCode.UsageOrConfig;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not read the investigation cases from the store: {ex.Message}");
            return CollectorToggleExitCode.StoreUnavailable;
        }
    }
}
