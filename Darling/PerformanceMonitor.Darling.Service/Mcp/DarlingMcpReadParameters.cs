/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The naive-UTC parameter-binding helpers the slice-3 diagnostic-depth readers share — the SAME
/// bindings the merged slice-2 <see cref="DarlingDataReader"/> defines privately, lifted into one
/// place so the four new domain readers (blocking, sessions, config-history, object-stats) bind
/// identically without re-declaring them per file. Every timestamp binds
/// <c>Kind=Unspecified</c> so it maps to the store's <c>timestamp without time zone</c> columns (a
/// <c>Kind=Utc</c> DateTime maps to timestamptz and throws since Npgsql 6.0); the nullable-text
/// binder activates a read's <c>$N::text IS NULL OR ...</c> "no filter" branch when the value is null.
/// </summary>
internal static class DarlingMcpReadParameters
{
    public static void AddInt(NpgsqlCommand command, int value) =>
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = value });

    public static void AddText(NpgsqlCommand command, string value) =>
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = value ?? "" });

    /// <summary>Binds a nullable text filter (a null value binds SQL NULL).</summary>
    public static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value });

    /// <summary>Binds a boolean flag — a read's <c>NOT $N::boolean OR ...</c> "filter off" branch (#3541 A13).</summary>
    public static void AddBoolean(NpgsqlCommand command, bool value) =>
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = value });

    /// <summary>Binds a naive-UTC timestamp (Kind=Unspecified → the store's timestamp columns).</summary>
    public static void AddTimestamp(NpgsqlCommand command, DateTime value) =>
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });

    /// <summary>Binds the common (server_id, window start, window end) parameter triple.</summary>
    public static void AddWindow(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc)
    {
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
    }
}
