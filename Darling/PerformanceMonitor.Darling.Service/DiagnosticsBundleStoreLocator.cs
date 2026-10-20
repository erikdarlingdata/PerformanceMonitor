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
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6217: finds the managed store for <c>--diagnostics-bundle</c> the way the running service sees it, and says WHY it
/// could not be reached.
///
/// <para>The bundle used to try <c>127.0.0.1:{postgres.port}</c> once and, on a refusal, report <c>connect_failed</c>.
/// That one code covered three different situations that want different action: the store is not running, the store
/// is still starting (a fresh install's first start, a restart, a quiesced TimescaleDB update on its private port),
/// and the bundle is looking at a port the live store is not on (the config was edited after the service started, or
/// the bundle read another copy of the config). The managed store's own <c>postmaster.pid</c> in the data directory
/// answers all three: its first line is the postmaster's PID, its fourth the port it listens on and its eighth the
/// state (<c>starting</c>, <c>ready</c>, <c>stopping</c>). The service reads the same file to find an orphaned
/// quiesced-start server.</para>
///
/// <para>Everything returned to the bundle is a fixed code. No path, port or address leaves this class.</para>
/// </summary>
internal static class DiagnosticsBundleStoreLocator
{
    /// <summary>What the store's data directory says about the postmaster.</summary>
    internal enum StoreState
    {
        /// <summary>The data directory does not exist: the bundle is not looking at the service's store.</summary>
        DataDirectoryMissing,

        /// <summary>No postmaster is alive on the data directory, or it is shutting down.</summary>
        NotRunning,

        /// <summary>A postmaster is alive but not ready: starting up, or on a quiesced start's private port.</summary>
        Starting,

        /// <summary>A postmaster is alive, ready, and on the configured port.</summary>
        ReadyOnConfiguredPort,

        /// <summary>A postmaster is alive, ready, and on a port other than the configured one.</summary>
        ReadyOnOtherPort,
    }

    internal readonly record struct Located(StoreState State, int Port);

    /// <summary>The outcome of <see cref="ConnectAsync"/>. <see cref="DataSource"/> is set when the store was reached.</summary>
    internal sealed record ConnectResult(
        NpgsqlDataSource? DataSource, Exception? Error, string? Reason, IReadOnlyList<string> Notes);

    /// <summary>Fixed reason codes this class adds to <see cref="DiagnosticsBundle.StoreReasonCodes"/>.</summary>
    internal const string ReasonDataDirectoryMissing = "data_directory_missing";
    internal const string ReasonNotRunning = "store_not_running";
    internal const string ReasonStarting = "store_starting";

    /// <summary>Said when the store was reached on the port its postmaster records rather than on the configured one.</summary>
    internal const string PortDiffersNote =
        "The managed store is listening on a different port than postgres.port, so the bundle connected there. "
        + "The service reads postgres.port only when it starts: restart it after changing the port.";

    /// <summary>The fixed reason code for a state, or null when the state does not explain a refusal.</summary>
    internal static string? ReasonFor(StoreState state) => state switch
    {
        StoreState.DataDirectoryMissing => ReasonDataDirectoryMissing,
        StoreState.NotRunning => ReasonNotRunning,
        StoreState.Starting => ReasonStarting,
        _ => null,
    };

    /// <summary>
    /// Reads <c>postmaster.pid</c> in <paramref name="dataDirectory"/>. <paramref name="isPostgresProcess"/> says whether a
    /// PID is a live <c>postgres</c> process (Windows reuses PIDs, so a pid file a crash left behind can name something
    /// else); null uses the real process table.
    /// </summary>
    internal static Located Locate(string dataDirectory, int configuredPort, Func<int, bool>? isPostgresProcess = null)
    {
        if (!Directory.Exists(dataDirectory))
        {
            return new Located(StoreState.DataDirectoryMissing, 0);
        }

        string[] lines;
        try
        {
            var pidFile = Path.Combine(dataDirectory, "postmaster.pid");
            if (!File.Exists(pidFile))
            {
                return new Located(StoreState.NotRunning, 0);
            }

            /* Shared for writing and deletion too, so a postmaster updating the file never blocks the read. */
            using var stream = new FileStream(pidFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            using var reader = new StreamReader(stream);
            var read = new List<string>();
            while (read.Count < 8 && reader.ReadLine() is { } line)
            {
                read.Add(line.Trim());
            }

            lines = read.ToArray();
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A file that cannot be read says nothing about the store: leave the plain connect_failed code. */
            return new Located(StoreState.ReadyOnConfiguredPort, configuredPort);
        }

        if (lines.Length == 0
            || !long.TryParse(lines[0], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var recorded)
            || recorded == 0
            || Math.Abs(recorded) > int.MaxValue)
        {
            return new Located(StoreState.NotRunning, 0);
        }

        /* A standalone (single-user) backend records its PID negated, and holds the directory just the same. */
        var pid = (int)Math.Abs(recorded);
        if (!(isPostgresProcess ?? IsPostgresProcess)(pid))
        {
            return new Located(StoreState.NotRunning, 0);
        }

        var port = lines.Length > 3 && int.TryParse(lines[3], NumberStyles.None, CultureInfo.InvariantCulture, out var p) ? p : configuredPort;
        var status = lines.Length > 7 ? lines[7] : "ready";
        if (string.Equals(status, "stopping", StringComparison.OrdinalIgnoreCase))
        {
            return new Located(StoreState.NotRunning, port);
        }

        if (string.Equals(status, "starting", StringComparison.OrdinalIgnoreCase) || IsQuiescedStart(dataDirectory, port))
        {
            return new Located(StoreState.Starting, port);
        }

        return new Located(port == configuredPort ? StoreState.ReadyOnConfiguredPort : StoreState.ReadyOnOtherPort, port);
    }

    /// <summary>
    /// A quiesced TimescaleDB update or auto.conf trial leaves a marker holding its private port. A server on that port is
    /// the service's own transient start, stopped before the normal start, not the store the bundle is after.
    /// </summary>
    private static bool IsQuiescedStart(string dataDirectory, int port)
    {
        /* The store is managed on Windows only, and so is the marker's owner. */
        if (!OperatingSystem.IsWindows())
        {
            return false;
        }

        try
        {
            var marker = Path.Combine(dataDirectory, DarlingStoreUpgrade.QuiescedUpdateMarkerFileName);
            return File.Exists(marker)
                && string.Equals(File.ReadAllText(marker).Trim(), port.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static bool IsPostgresProcess(int pid)
    {
        try
        {
            using var process = System.Diagnostics.Process.GetProcessById(pid);
            return string.Equals(process.ProcessName, "postgres", StringComparison.OrdinalIgnoreCase);
        }
        catch (Exception ex) when (ex is ArgumentException or InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>The same connection string on another port.</summary>
    internal static string WithPort(string connectionString, int port) =>
        new NpgsqlConnectionStringBuilder(connectionString) { Port = port }.ConnectionString;

    /// <summary>
    /// A refusal that says nothing was listening or ready: a socket error, a connect timeout, or PostgreSQL's own
    /// "the database system is starting up" (57P03). Anything else (a rejected login, a missing database) is not about
    /// where the store is, and is left to the plain classification.
    /// </summary>
    internal static bool IsNotListening(Exception? error)
    {
        for (var ex = error; ex is not null; ex = ex.InnerException)
        {
            if (ex is PostgresException pg)
            {
                return string.Equals(pg.SqlState, "57P03", StringComparison.Ordinal);
            }

            if (ex is SocketException or TimeoutException or OperationCanceledException)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Opens the managed store, trying the configured port first. When it is refused, reads the store's own state: a
    /// live store on another port is used (the service's store, wherever it listens), a store still starting is waited
    /// for up to <paramref name="startWait"/>, and anything else becomes a fixed reason code that tells "the store is
    /// not running" from "the bundle looked in the wrong place". <paramref name="open"/> opens one data source on the
    /// given port, and throws when it cannot.
    /// </summary>
    internal static async Task<ConnectResult> ConnectAsync(
        string dataDirectory,
        int configuredPort,
        Func<int, CancellationToken, Task<NpgsqlDataSource>> open,
        TimeSpan startWait,
        TimeSpan pause,
        CancellationToken cancellationToken,
        Func<int, bool>? isPostgresProcess = null)
    {
        var notes = new List<string>();
        var port = configuredPort;
        var triedOther = new HashSet<int>();
        var deadline = DateTime.UtcNow + startWait;
        Exception? error;
        Located located;
        while (true)
        {
            try
            {
                var source = await open(port, cancellationToken);
                if (port != configuredPort)
                {
                    notes.Add(PortDiffersNote);
                }

                return new ConnectResult(source, null, null, notes);
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                error = ex;
            }

            if (!IsNotListening(error))
            {
                return new ConnectResult(null, error, null, notes);
            }

            located = Locate(dataDirectory, configuredPort, isPostgresProcess);
            if (located.State == StoreState.ReadyOnOtherPort && triedOther.Add(located.Port))
            {
                port = located.Port;
                continue;
            }

            if (located.State == StoreState.Starting && DateTime.UtcNow < deadline)
            {
                await Task.Delay(pause, cancellationToken);
                port = configuredPort;
                continue;
            }

            break;
        }

        return new ConnectResult(null, error, ReasonFor(located.State), notes);
    }
}
