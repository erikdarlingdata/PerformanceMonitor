/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// #9195: DuckDB's own operator profile of the slow reads, for an install where nobody can run EXPLAIN ANALYZE against the store's
/// file. Off by default (settings.json <see cref="SettingsKey"/>). When it is on, a read that goes through
/// <see cref="ExecuteProfiledReaderAsync"/> or <see cref="ExecuteProfiledScalarAsync"/> runs with profiling on, on its own
/// connection, written as JSON to <c>duckdb-profiles</c> under the logs folder, one file per statement. Profiling is turned off again on
/// that connection straight after the statement, so a connection that goes back to the pool never carries it. Off, the two methods run
/// the statement and nothing else: no extra statement, no folder.
///
/// <para>A profile is a diagnostic, so it never fails a read: a folder that cannot be written or a setting DuckDB refuses is logged
/// once at Warning and the statement runs plain.</para>
/// </summary>
internal static class DuckDbReadProfile
{
    /// <summary>The settings.json key.</summary>
    internal const string SettingsKey = "duckdb_profile_slow_reads";

    /// <summary>How many files of one read name are kept. The oldest go when a newer one is written.</summary>
    internal const int KeepPerRead = 20;

    internal const string FolderName = "duckdb-profiles";

    /// <summary>Set once at startup from settings.json. A test sets it directly.</summary>
    internal static bool Enabled { get; set; }

    /// <summary>
    /// A test seam, per async flow so tests that run side by side never see each other's switch: the switch's state and the folder the
    /// profiles go to, in place of <see cref="Enabled"/> and <c>duckdb-profiles</c> under the logs folder.
    /// </summary>
    private static readonly AsyncLocal<(bool On, string Folder)?> s_testScope = new();

    internal static IDisposable ScopeForTests(bool on, string folder)
    {
        var outer = s_testScope.Value;
        s_testScope.Value = (on, folder);
        return new TestScope(outer);
    }

    private sealed class TestScope(( bool On, string Folder)? outer) : IDisposable
    {
        public void Dispose() => s_testScope.Value = outer;
    }

    private static bool IsOn => s_testScope.Value is { } scope ? scope.On : Enabled;

    private static int s_warned;
    private static int s_shapeLogged;

    internal static string? Folder()
    {
        if (s_testScope.Value is { } scope) return scope.Folder;
        var logs = AppLogger.GetLogDirectory();
        return string.IsNullOrEmpty(logs) ? null : Path.Combine(logs, FolderName);
    }

    /// <summary>Runs <paramref name="command"/> as a reader, profiled under <paramref name="readName"/> when the switch is on.</summary>
    internal static async Task<DbDataReader> ExecuteProfiledReaderAsync(this DbCommand command, string readName, CancellationToken cancellationToken = default)
    {
        if (!IsOn) return await command.ExecuteReaderAsync(cancellationToken);

        var scope = Begin(command.Connection, readName);
        try
        {
            return await command.ExecuteReaderAsync(cancellationToken);
        }
        finally
        {
            scope?.End();
        }
    }

    /// <summary>Runs <paramref name="command"/> as a scalar, profiled under <paramref name="readName"/> when the switch is on.</summary>
    internal static async Task<object?> ExecuteProfiledScalarAsync(this DbCommand command, string readName, CancellationToken cancellationToken = default)
    {
        if (!IsOn) return await command.ExecuteScalarAsync(cancellationToken);

        var scope = Begin(command.Connection, readName);
        try
        {
            return await command.ExecuteScalarAsync(cancellationToken);
        }
        finally
        {
            scope?.End();
        }
    }

    private static Scope? Begin(DbConnection? connection, string readName)
    {
        if (connection is null) return null;

        readName = Regex.Replace(readName, "[^A-Za-z0-9_-]", "_");
        string? path;
        var profilingOn = false;
        try
        {
            LogShapeOnce(connection);
            var folder = Folder() ?? throw new IOException("Lite has no logs folder to write profiles to.");
            Directory.CreateDirectory(folder);
            path = Path.Combine(folder, string.Create(CultureInfo.InvariantCulture, $"{readName}-{DateTime.UtcNow:yyyyMMddTHHmmssfff}.json"));
            /* The mode comes first: DuckDB checks the output file's extension against the mode when the output is set, and the
               default mode wants .txt. */
            RunSet(connection, "SET enable_profiling = 'json'");
            profilingOn = true;
            RunSet(connection, $"SET profiling_output = '{path.Replace("'", "''")}'");
            return new Scope(connection, readName, path);
        }
        catch (Exception ex)
        {
            WarnOnce($"DuckDB profile for '{readName}' could not be set up: {ex.Message}");
            if (profilingOn) TryDisable(connection);
            return null;
        }
    }

    private static void RunSet(DbConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void TryDisable(DbConnection connection)
    {
        try
        {
            RunSet(connection, "PRAGMA disable_profiling");
        }
        catch (Exception ex)
        {
            WarnOnce($"DuckDB profiling could not be turned off again: {ex.Message}");
        }
    }

    /// <summary>Once per process, the first time the switch is on: the machine shape a field timing has to be read against.</summary>
    private static void LogShapeOnce(DbConnection connection)
    {
        if (Interlocked.Exchange(ref s_shapeLogged, 1) != 0) return;
        try
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT current_setting('threads'), current_setting('memory_limit')";
            using var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                AppLogger.Info("DuckDbReadProfile",
                    string.Create(CultureInfo.InvariantCulture,
                        $"DuckDB read profiles are on ({SettingsKey}). Machine: {Environment.ProcessorCount} logical processors, DuckDB threads {reader.GetValue(0)}, memory_limit {reader.GetValue(1)}."));
            }
        }
        catch (Exception ex)
        {
            WarnOnce($"DuckDB threads and memory_limit could not be read: {ex.Message}");
        }
    }

    private static void WarnOnce(string message)
    {
        if (Interlocked.Exchange(ref s_warned, 1) == 0) AppLogger.Warn("DuckDbReadProfile", message);
    }

    /// <summary>Test seam: lets the next failure and the shape line show again.</summary>
    internal static void ResetForTests()
    {
        s_warned = 0;
        s_shapeLogged = 0;
    }

    private sealed class Scope
    {
        private readonly DbConnection _connection;
        private readonly string _readName;
        private readonly string _path;

        internal Scope(DbConnection connection, string readName, string path)
        {
            _connection = connection;
            _readName = readName;
            _path = path;
        }

        /// <summary>Profiling off on the connection, the file named on the current trail, and the read's older files pruned.</summary>
        internal void End()
        {
            TryDisable(_connection);
            DuckDbProfileTrail.Add(Path.GetFileName(_path));
            try
            {
                Prune(Path.GetDirectoryName(_path)!, _readName);
            }
            catch (Exception ex)
            {
                WarnOnce($"DuckDB profiles for '{_readName}' could not be pruned: {ex.Message}");
            }
        }
    }

    /// <summary>Deletes every file of <paramref name="readName"/> but its <see cref="KeepPerRead"/> newest. The stamp sorts as text.</summary>
    internal static void Prune(string folder, string readName)
    {
        var pattern = new Regex("^" + Regex.Escape(readName) + @"-\d{8}T\d{9}\.json$", RegexOptions.CultureInvariant);
        var old = Directory.EnumerateFiles(folder, readName + "-*.json")
            .Where(f => pattern.IsMatch(Path.GetFileName(f)))
            .OrderByDescending(f => Path.GetFileName(f), StringComparer.Ordinal)
            .Skip(KeepPerRead)
            .ToList();
        foreach (var file in old)
        {
            File.Delete(file);
        }
    }
}

/// <summary>
/// #9195: the profile files one call wrote, so its slow-read line can name them. Same shape as <see cref="ReadLockWaitTally"/>: a call
/// opens a trail with <see cref="Begin"/>, a profiled read adds its file to the current trail and every trail around it, and the trail
/// flows with the async call. A call that wrote none has an empty <see cref="Describe"/>, so the line is unchanged with the switch off.
/// </summary>
internal sealed class DuckDbProfileTrail : IDisposable
{
    private static readonly AsyncLocal<DuckDbProfileTrail?> s_current = new();

    private readonly DuckDbProfileTrail? _outer;
    private readonly List<string> _files = [];
    private bool _disposed;

    private DuckDbProfileTrail()
    {
        _outer = s_current.Value;
    }

    internal static DuckDbProfileTrail Begin()
    {
        var trail = new DuckDbProfileTrail();
        s_current.Value = trail;
        return trail;
    }

    internal static void Add(string file)
    {
        for (var t = s_current.Value; t is not null; t = t._outer)
        {
            lock (t._files) t._files.Add(file);
        }
    }

    internal IReadOnlyList<string> Files
    {
        get
        {
            lock (_files) return _files.ToList();
        }
    }

    /// <summary><c>, profile files in duckdb-profiles: a.json, b.json</c>, or nothing when the call wrote none.</summary>
    internal string Describe()
    {
        var files = Files;
        return files.Count == 0 ? "" : $", profile files in {DuckDbReadProfile.FolderName}: {string.Join(", ", files)}";
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        s_current.Value = _outer;
    }
}
