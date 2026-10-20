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

        if (InvestigationFeedbackExport.FilterRefusal(since, until, verdict) is { } refusal)
        {
            errorMessage = refusal;
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

        if (!TryCheckInvestigationExportTarget(args!, error))
        {
            return CollectorToggleExitCode.UsageOrConfig;
        }

        var mode = McpDataDetail.Full;
        try
        {
            mode = ReadConfiguredDataDetail(args!.ConfigPath);
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

    /// <summary>
    /// Decides, before anything is read, whether the export file can be written (#6236): refuses a folder, a link or junction, a
    /// folder that does not exist, and (without <c>--force</c>) a file that is already there. Writes nothing.
    /// </summary>
    private static bool TryCheckInvestigationExportTarget(ExportInvestigationsArgs args, TextWriter error)
    {
        var path = args.FilePath;
        if (path.EndsWith(Path.DirectorySeparatorChar) || path.EndsWith(Path.AltDirectorySeparatorChar) || Directory.Exists(path))
        {
            error.WriteLine($"'{path}' is a folder. Name the file to write inside it.");
            return false;
        }

        var directory = Path.GetDirectoryName(Path.GetFullPath(path));
        if (directory is null || !Directory.Exists(directory))
        {
            error.WriteLine($"The folder for '{path}' does not exist. Create it first.");
            return false;
        }

        FileAttributes attributes;
        try
        {
            attributes = File.GetAttributes(path);
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            error.WriteLine($"Could not inspect '{path}': {ex.Message}");
            return false;
        }

        if (attributes.HasFlag(FileAttributes.ReparsePoint))
        {
            error.WriteLine($"'{path}' is a link, so the file would land somewhere else. Name a plain file.");
            return false;
        }

        if (attributes.HasFlag(FileAttributes.Directory))
        {
            error.WriteLine($"'{path}' is a folder. Name the file to write inside it.");
            return false;
        }

        if (!args.Force)
        {
            error.WriteLine($"'{path}' already exists. Pick another file, or add --force to replace it.");
            return false;
        }

        return true;
    }

    /// <summary>The exit code of an export the operator cancelled with Ctrl+C: the shell's own number for it (#6236).</summary>
    internal const int InvestigationExportCancelledExitCode = 130;

    /// <summary>The export on an open store, so a live test drives exactly what the command line does.</summary>
    internal static async Task<int> ExportInvestigationsAsync(
        NpgsqlDataSource dataSource, ExportInvestigationsArgs args, McpDataDetail mode, TextWriter output, TextWriter error,
        CancellationToken cancellationToken)
    {
        if (!TryCheckInvestigationExportTarget(args, error))
        {
            return CollectorToggleExitCode.UsageOrConfig;
        }

        var path = Path.GetFullPath(args.FilePath);
        var callerToken = cancellationToken;

        /* Ctrl+C cancels the export through the token (#6236), so the unwinding below deletes the partial file: without the handler the
           process ends where it stands and the hidden partial stays beside the target. */
        using var pressed = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var cancelKey = InvestigationExportCancelKey.Subscribe(pressed.Cancel);
        cancellationToken = pressed.Token;

        /* The partial file sits beside the target under a random name, created new (so a planted file or link at that name makes
           the create fail rather than be written through). Who can read it is InvestigationExportFile.ReadableBy. It is deleted on
           every way out but the move that makes it the export; when it cannot be deleted the path is printed. */
        var temp = Path.Combine(
            Path.GetDirectoryName(path)!, "." + Path.GetFileName(path) + "." + Guid.NewGuid().ToString("N") + ".partial");
        try
        {
            var (serverId, serverLabel, serverError) = await InvestigationFeedbackExport.ResolveServerAsync(dataSource, args.ServerName, cancellationToken);
            if (serverError is not null)
            {
                error.WriteLine(serverError);
                return CollectorToggleExitCode.UsageOrConfig;
            }

            var owner = new StoreOwnerSource();
            owner.Publish(dataSource);
            using var keys = new PseudonymKeySource(owner);
            var key = await PseudonymKeySources.KeyForAsync(keys, mode, cancellationToken);
            var store = new InvestigationRequestStore(dataSource, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);
            InvestigationExportOutcome outcome;
            await using (var stream = InvestigationExportFile.CreateOwnerOnly(temp))
            {
                outcome = await InvestigationFeedbackExport.WriteAsync(
                    store, args.SinceUtc, args.UntilUtc, serverId, args.Verdict, mode, key, stream, cancellationToken);
            }

            if (outcome.Refused)
            {
                error.WriteLine(InvestigationFeedbackExport.RefusalSentence);
                return CollectorToggleExitCode.StoreUnavailable;
            }

            /* No overwrite unless --force: a file that appeared at the target while the store was read makes this fail. */
            File.Move(temp, path, overwrite: args.Force);
            output.WriteLine($"Wrote {outcome.Cases} investigation case(s) with feedback to {path} (one JSON object per line, masked at {McpDataDetailModes.Word(mode)}, readable by {InvestigationExportFile.ReadableBy}).");
            if (serverLabel is not null)
            {
                output.WriteLine($"Server: {serverLabel}.");
            }

            if (outcome.HeldBack > 0)
            {
                output.WriteLine($"{outcome.HeldBack} case(s) a live practice request replays were held back.");
            }

            if (outcome.Truncated)
            {
                output.WriteLine($"The filter held more than {InvestigationRequestStore.FeedbackExportLimit} cases; these are the newest. Narrow it with --since, --until, --server or --verdict for the rest.");
            }

            return CollectorToggleExitCode.Success;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            error.WriteLine($"Could not write '{path}': {ex.Message}");
            return CollectorToggleExitCode.UsageOrConfig;
        }
        catch (OperationCanceledException) when (!callerToken.IsCancellationRequested)
        {
            error.WriteLine("Cancelled. No export file was written.");
            return InvestigationExportCancelledExitCode;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            error.WriteLine($"Could not read the investigation cases from the store: {ex.Message}");
            return CollectorToggleExitCode.StoreUnavailable;
        }
        finally
        {
            if (!InvestigationExportFile.TryDelete(temp))
            {
                error.WriteLine($"Could not remove the partial file {temp}; delete it.");
            }
        }
    }
}
