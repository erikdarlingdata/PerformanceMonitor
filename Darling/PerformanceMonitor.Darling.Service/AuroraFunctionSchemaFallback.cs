/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6143: where to call an Aurora built-in function when <c>pg_catalog.pg_proc</c> does not show it. #2340 measured a
/// <c>pg_proc</c> lookup that returned no <c>aurora_version</c> row on a real Aurora cluster for a pg_monitor login,
/// so on Aurora the lookup can come back empty for every function. The call then goes to <c>pg_catalog</c>; when that
/// fails with 42883 (undefined_function), it is tried once in the schema the pg_stat_statements extension was created
/// in. No other schema is ever tried.
/// <para>The test is <c>EXPLAIN</c> of the call, which resolves the function exactly as the call does but runs
/// nothing. The schema that worked is remembered per target for <see cref="RememberFor"/>, so a run does not pay for
/// the test again.</para>
/// </summary>
internal static class AuroraFunctionSchemaFallback
{
    /// <summary>How long a target's answer is kept before it is tested again; settable for tests.</summary>
    internal static TimeSpan RememberFor { get; set; } = TimeSpan.FromHours(1);

    private static readonly ConcurrentDictionary<(string Target, string Function, string StatementsSchema), (string Schema, DateTime CheckedAtUtc)> s_remembered = new();

    /// <summary>The arguments each function is called with in collector SQL, so the test resolves the same signature.</summary>
    private static string Arguments(string functionName) =>
        string.Equals(functionName, "aurora_stat_statements", StringComparison.Ordinal) ? "(false)" : "()";

    /// <summary>The test statement: EXPLAIN plans the call (which resolves the function) and runs nothing.</summary>
    internal static string ProbeSql(string quotedSchema, string functionName) =>
        "EXPLAIN (COSTS OFF) SELECT * FROM " + quotedSchema + "." + functionName + Arguments(functionName);

    /// <summary>The schema remembered for this target and function, or null. For tests and the log.</summary>
    internal static string? Remembered(string targetKey, string functionName, string statementsSchema) =>
        s_remembered.TryGetValue((targetKey, functionName, statementsSchema), out var entry)
        && DateTime.UtcNow - entry.CheckedAtUtc < RememberFor
            ? entry.Schema
            : null;

    /// <summary>Forgets every remembered answer; for tests.</summary>
    internal static void ForgetAll() => s_remembered.Clear();

    /// <summary>
    /// <paramref name="chosen"/> (from <see cref="CollectorContext.ChoosePgFunctionSchemas"/>) plus, for each of
    /// <paramref name="functionNames"/> it does not name, the schema whose call resolves: <c>pg_catalog</c> first,
    /// then <paramref name="statementsSchema"/>. A function neither resolves in is left out, so the collector calls it
    /// in <c>pg_catalog</c> and fails there as before. With no <paramref name="statementsSchema"/> (or one that is
    /// <c>pg_catalog</c>) there is nothing to choose between and nothing is tested.
    /// <paramref name="targetKey"/> null means nothing is remembered (the connect-time probe, once per connection).
    /// </summary>
    internal static async Task<IReadOnlyDictionary<string, string>> FillAsync(
        DbConnection connection,
        IReadOnlyDictionary<string, string> chosen,
        IEnumerable<string> functionNames,
        string? statementsSchema,
        string? targetKey,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(chosen);
        ArgumentNullException.ThrowIfNull(functionNames);
        if (statementsSchema is null || string.Equals(statementsSchema, "pg_catalog", StringComparison.Ordinal))
        {
            return chosen;
        }

        Dictionary<string, string>? filled = null;
        foreach (var function in functionNames)
        {
            if (chosen.ContainsKey(function))
            {
                continue;
            }

            var schema = targetKey is null ? null : Remembered(targetKey, function, statementsSchema);
            if (schema is null)
            {
                schema = await ProbeAsync(connection, function, statementsSchema, logger, cancellationToken);
                if (schema is null)
                {
                    continue;
                }

                if (targetKey is not null)
                {
                    s_remembered[(targetKey, function, statementsSchema)] = (schema, DateTime.UtcNow);
                }
            }

            /* pg_catalog is where a function missing from the map is called anyway. */
            if (string.Equals(schema, "pg_catalog", StringComparison.Ordinal))
            {
                continue;
            }

            filled ??= new Dictionary<string, string>(chosen, StringComparer.Ordinal);
            filled[function] = schema;
        }

        return filled ?? chosen;
    }

    /// <summary>
    /// <c>pg_catalog</c> when the call resolves there, else <paramref name="statementsSchema"/> when it resolves
    /// there, else <c>pg_catalog</c> (neither resolved: the collector fails there as before, and that answer is
    /// remembered too). Null when a test failed for any reason other than 42883: nothing is remembered and the
    /// collector calls <c>pg_catalog</c>.
    /// </summary>
    private static async Task<string?> ProbeAsync(
        DbConnection connection, string functionName, string statementsSchema, ILogger? logger, CancellationToken cancellationToken)
    {
        foreach (var schema in new[] { "pg_catalog", statementsSchema })
        {
            try
            {
                using var command = connection.CreateCommand();
                command.CommandTimeout = DarlingCollectorRunner.PgExtensionSchemasTimeoutSeconds;
                command.CommandText = ProbeSql(PgIdentifier.Quote(schema), functionName);
                await command.ExecuteNonQueryAsync(cancellationToken);
                return schema;
            }
            catch (DbException ex) when (string.Equals(ex.SqlState, "42883", StringComparison.Ordinal))
            {
                logger?.LogDebug("{Function} does not resolve in schema {Schema} (#6143)", functionName, schema);
            }
            catch (DbException ex)
            {
                logger?.LogDebug(ex, "Testing where {Function} lives did not succeed (#6143)", functionName);
                return null;
            }
        }

        return "pg_catalog";
    }
}
