/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's startup self-description (#1954): where darling.json came from, and what the viewer
/// actually parsed out of it — everything except credentials.
///
/// <para><b>Why this exists.</b> Field reports of "the viewer will not connect" were undiagnosable from the
/// log, because the viewer said neither which of its four candidate config paths it read nor what it got.
/// Two very different faults — reading the WRONG file, and reading the RIGHT file with a wrong value —
/// produced the identical generic failure. The certificate is the sharpest case: the documented
/// bring-your-own connection string carries a bare <c>Root Certificate=server.crt</c>, and a viewer launched
/// from a shortcut and one launched from a shell used to look for that cert in different places, with
/// neither one saying where. #1970 removed the divergence — a relative certificate is anchored to
/// darling.json's own directory — and this block reports the anchored result, through the SAME
/// <see cref="ViewerCertificateAnchor"/> the connection string is rewritten by, so the path it names is the
/// path Npgsql opens rather than a second opinion about it.</para>
///
/// <para><b>Redaction is structural, not careful.</b> The summary is built from an ALLOWLIST of connection-string
/// properties (host, port, username, database, SSL mode, search path, root certificate) read off a parsed
/// <see cref="NpgsqlConnectionStringBuilder"/>. There is no path through this class that copies the caller's
/// connection string into its output, so no key an operator adds later — <c>Password</c>, <c>Passfile</c>, a
/// future credential keyword — can leak by being forgotten. A string that will not parse reports only the
/// exception TYPE for the same reason: an Npgsql parse message can quote the offending fragment.
/// <c>ViewerConfigDiagnosticsTests</c> pins this by feeding a live password through and asserting it never
/// appears in the output.</para>
/// </summary>
public static class ViewerConfigDiagnostics
{
    /// <summary>Label column width, so the block lines up when rendered in the log and the failure overlay.</summary>
    private const int LabelWidth = 20;

    /// <summary>The log source every diagnostic line is written under, so an operator can grep one tag.</summary>
    public const string LogSource = "Config";

    /// <summary>
    /// The one hint for a missing darling.json (#6097), shared by the window and the headless self-test. It names
    /// the places the viewer looked, then the two supported ways to get the file: the sample on the service
    /// machine, or --export-viewer-config on the service host for a viewer on another machine.
    /// </summary>
    public const string ConfigNotFoundMessage =
        "darling.json was not found beside the viewer, one folder up, in the installed Darling service's folder, or at DARLING_CONFIG / --config. "
        + "On the service machine, create it from darling.sample.json. "
        + "On another machine, run PerformanceMonitor.Darling.Service.exe --export-viewer-config on the service host "
        + "and copy the folder it writes beside the viewer.";

    /// <summary>
    /// Which darling.json the viewer resolved, by which rule, and whether it is there. Emitted BEFORE the
    /// load attempt, so a file that does not exist or will not parse still tells the operator where the
    /// viewer looked.
    /// </summary>
    public static IReadOnlyList<string> DescribeConfigLocation(ViewerConfigLocation location)
    {
        ArgumentNullException.ThrowIfNull(location);

        var lines = new List<string>
        {
            Line("darling.json source", SourceLabel(location.Source)),
            Line("darling.json path", location.FullPath),
        };

        /* Only worth a second line when the two differ — a relative DARLING_CONFIG or command-line path is
           precisely the case where the operator's mental path and the viewer's are not the same string. */
        if (!string.Equals(location.Path, location.FullPath, StringComparison.Ordinal))
        {
            lines.Add(Line("  as configured", location.Path));
        }

        lines.Add(Line("darling.json exists", location.Exists ? "yes" : "NO"));
        return lines;
    }

    /// <summary>
    /// The non-secret parse summary: what the viewer will actually connect with. <paramref name="managed"/>
    /// comes from the config rather than the connection string (in managed mode the string is derived, so
    /// <c>postgres.connectionString</c> in the file is not consulted at all — worth saying out loud).
    /// <paramref name="configDirectory"/> is darling.json's own directory — the anchor a relative
    /// <c>Root Certificate</c> resolves against (#1970); null means the process working directory, the
    /// unanchored fallback <see cref="ViewerCertificateAnchor.Resolve"/> and Npgsql both land on.
    /// </summary>
    public static IReadOnlyList<string> DescribeConnection(
        string? connectionString, bool managed, string? configDirectory = null)
    {
        /* No connection string means the file was never loaded (missing, or it threw). Say only that —
           reporting a managed flag or a set of fields nothing was parsed from would be an invention. */
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return new[] { Line("connection", "(darling.json was not loaded — nothing was parsed)") };
        }

        var lines = new List<string>
        {
            Line(
                "postgres.managed",
                managed
                    ? "true (connection derived by the viewer; postgres.connectionString is not read)"
                    : "false (postgres.connectionString used verbatim)"),
        };

        NpgsqlConnectionStringBuilder builder;
        try
        {
            builder = new NpgsqlConnectionStringBuilder(connectionString);
        }
        catch (Exception ex)
        {
            /* Type only, never the message: Npgsql's parse errors can quote the fragment they choked on,
               and that fragment can be the credential half of the string. */
            lines.Add(Line(
                "connection",
                $"COULD NOT BE PARSED ({ex.GetType().Name}) — check postgres.connectionString in darling.json"));
            return lines;
        }

        /* A string that parses but that Npgsql cannot build a data source from (a Host value with a ':' tail, a port out of
           range) is described by the formatter's text. Its Host would otherwise be printed as written, and the text after
           the ':' can be the tail of a password. A string with no Host is the formatter's sentence too. */
        if (DarlingStoreConnection.TryDescribeParseFailure(connectionString) is { } unusable)
        {
            lines.Add(Line("connection", $"COULD NOT BE USED — {unusable}"));
            return lines;
        }

        lines.Add(Line("Host", Present(builder.Host)));
        lines.Add(Line("Port", builder.Port.ToString(CultureInfo.InvariantCulture)));
        lines.Add(Line("Username", Present(builder.Username)));
        lines.Add(Line("Database", Present(builder.Database)));
        lines.Add(Line("SSL Mode", builder.SslMode.ToString()));
        lines.Add(Line("Search Path", Present(builder.SearchPath)));
        lines.AddRange(DescribeRootCertificate(builder.RootCertificate, configDirectory));
        return lines;
    }

    /// <summary>
    /// The whole block, as it goes to the log and into the connection-failure overlay. Pass a null
    /// <paramref name="connectionString"/> for the pre-load failures (missing / unreadable file), where the
    /// location lines are the entire honest answer.
    /// </summary>
    public static string BuildDetails(
        ViewerConfigLocation location, string? connectionString, bool managed, string? configDirectory = null)
    {
        var lines = new List<string>(DescribeConfigLocation(location));
        lines.AddRange(DescribeConnection(connectionString, managed, configDirectory));
        return string.Join(Environment.NewLine, lines);
    }

    /// <summary>
    /// The certificate half: the value as written, the absolute path Npgsql will actually open, and whether
    /// that file is there. Verify-full against a cert that is not where the string says is one of the two
    /// failures this whole feature exists to name.
    /// </summary>
    private static IReadOnlyList<string> DescribeRootCertificate(string? rootCertificate, string? configDirectory)
    {
        if (string.IsNullOrWhiteSpace(rootCertificate))
        {
            return new[] { Line("Root Certificate", "(not set)") };
        }

        var lines = new List<string> { Line("Root Certificate", rootCertificate) };

        /* Through the SHARED resolver, never a second copy of the rule: this line promises the absolute path
           Npgsql will actually open, and the only way it can keep that promise is to ask the same code that
           rewrites the connection string (#1970). */
        var resolved = ViewerCertificateAnchor.Resolve(rootCertificate, configDirectory);
        if (resolved is null)
        {
            lines.Add(Line("  resolves to", "(not a usable path)"));
            return lines;
        }

        lines.Add(Line("  resolves to", resolved));
        if (!Path.IsPathFullyQualified(rootCertificate))
        {
            lines.Add(Line("  relative to", configDirectory ?? Environment.CurrentDirectory));
        }

        lines.Add(Line("  exists", File.Exists(resolved) ? "yes" : "NO"));
        return lines;
    }

    private static string SourceLabel(ViewerConfigSource source) => source switch
    {
        ViewerConfigSource.CommandLine => "command-line argument (outranks DARLING_CONFIG)",
        ViewerConfigSource.EnvironmentVariable => "DARLING_CONFIG environment variable",
        ViewerConfigSource.BesideViewer => "beside the viewer executable",
        ViewerConfigSource.ServiceRoot => "the service root, one level above the viewer",
        ViewerConfigSource.InstalledService => "the installed Darling service's folder (from its registered ImagePath)",
        _ => source.ToString(),
    };

    private static string Line(string label, string value) =>
        string.Concat((label + ":").PadRight(LabelWidth), " ", value);

    private static string Present(string? value) => string.IsNullOrWhiteSpace(value) ? "(not set)" : value;
}
