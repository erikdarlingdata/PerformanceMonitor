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
/// so on Aurora the lookup can come back empty for every function.
/// <para>The call is first tested in <c>pg_catalog</c> with <c>EXPLAIN</c>, which resolves the function exactly as the
/// call does. Only superusers can create objects in <c>pg_catalog</c>, so that is the one schema where planning a call
/// is safe: planning can run a function (PostgreSQL evaluates an IMMUTABLE call with constant arguments while it
/// plans), so a function in a schema other logins can create objects in is never planned or called.</para>
/// <para>When the call does not resolve in <c>pg_catalog</c> (42883, undefined_function), only
/// <see cref="CollectorContext.StatementsExtensionAuroraFunction"/>, which the pg_stat_statements extension itself
/// ships, is looked for further: in the extension's schema, and only as a member of the extension (<c>pg_depend</c>)
/// with the argument types the collector passes. That is a catalog read; nothing in that schema is planned or run to
/// find out. Any other function, or a same-named function that is not a member, leaves the collector calling
/// <c>pg_catalog</c>, as on stock PostgreSQL. No other schema is ever tried.</para>
/// <para>The answer is remembered per target for <see cref="RememberFor"/>, so a run does not pay for the test
/// again.</para>
/// </summary>
internal static class AuroraFunctionSchemaFallback
{
    /// <summary>How long a target's answer is kept before it is tested again; settable for tests.</summary>
    internal static TimeSpan RememberFor { get; set; } = TimeSpan.FromHours(1);

    private static readonly ConcurrentDictionary<(string Target, string Function, string StatementsSchema), (string Schema, DateTime CheckedAtUtc)> s_remembered = new();

    /// <summary>The arguments each function is called with in collector SQL, so the test resolves the same signature.</summary>
    private static string Arguments(string functionName) =>
        string.Equals(functionName, CollectorContext.StatementsExtensionAuroraFunction, StringComparison.Ordinal) ? "(false)" : "()";

    /// <summary>The <c>pg_catalog</c> test: EXPLAIN plans the call, which resolves the function. It is never built
    /// for any other schema.</summary>
    internal static string ProbeSql(string functionName) =>
        "EXPLAIN (COSTS OFF) SELECT * FROM pg_catalog." + functionName + Arguments(functionName);

    /// <summary>
    /// Every function named <see cref="CollectorContext.StatementsExtensionAuroraFunction"/> in the schema the
    /// pg_stat_statements extension was created in, with its argument types and whether <c>pg_depend</c> shows it is a
    /// member of that extension. A catalog read: nothing is planned or run. Rows that do not match are read so the log
    /// can say what was found.
    /// </summary>
    internal const string StatementsExtensionMemberSql = @"SELECT n.nspname::text, pg_catalog.oidvectortypes(p.proargtypes)::text,
       EXISTS (
           SELECT 1
           FROM pg_catalog.pg_depend AS d
           WHERE d.classid OPERATOR(pg_catalog.=) 'pg_catalog.pg_proc'::pg_catalog.regclass
           AND d.objid OPERATOR(pg_catalog.=) p.oid
           AND d.refclassid OPERATOR(pg_catalog.=) 'pg_catalog.pg_extension'::pg_catalog.regclass
           AND d.refobjid OPERATOR(pg_catalog.=) x.oid
           AND d.deptype OPERATOR(pg_catalog.=) 'e')
FROM pg_catalog.pg_extension AS x
JOIN pg_catalog.pg_proc AS p ON p.pronamespace OPERATOR(pg_catalog.=) x.extnamespace
JOIN pg_catalog.pg_namespace AS n ON n.oid OPERATOR(pg_catalog.=) p.pronamespace
WHERE x.extname OPERATOR(pg_catalog.=) 'pg_stat_statements'
AND p.proname::text OPERATOR(pg_catalog.=) '" + CollectorContext.StatementsExtensionAuroraFunction + "'";

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
    /// <paramref name="functionNames"/> it does not name, where its call resolves: <c>pg_catalog</c> first, then, for
    /// <see cref="CollectorContext.StatementsExtensionAuroraFunction"/> only, <paramref name="statementsSchema"/> when the
    /// function there is a member of pg_stat_statements. A function found in neither is left out, so the collector
    /// calls it in <c>pg_catalog</c> and fails there as before. With no <paramref name="statementsSchema"/> (or one that
    /// is <c>pg_catalog</c>) there is nothing to choose between and nothing is tested.
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
    /// <c>pg_catalog</c> when the call resolves there; else <paramref name="statementsSchema"/> when the function is
    /// <see cref="CollectorContext.StatementsExtensionAuroraFunction"/> and a member of pg_stat_statements there with the
    /// collector's argument types; else <c>pg_catalog</c> (the collector fails there as before, and that answer is
    /// remembered too). Null when a test failed for any reason other than 42883: nothing is remembered and the
    /// collector calls <c>pg_catalog</c>.
    /// </summary>
    private static async Task<string?> ProbeAsync(
        DbConnection connection, string functionName, string statementsSchema, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandTimeout = DarlingCollectorRunner.PgExtensionSchemasTimeoutSeconds;
            command.CommandText = ProbeSql(functionName);
            await command.ExecuteNonQueryAsync(cancellationToken);
            return "pg_catalog";
        }
        catch (DbException ex) when (string.Equals(ex.SqlState, "42883", StringComparison.Ordinal))
        {
            logger?.LogDebug("{Function} does not resolve in pg_catalog (#6143)", functionName);
        }
        catch (DbException ex)
        {
            logger?.LogDebug(ex, "Testing where {Function} lives did not succeed (#6143)", functionName);
            return null;
        }

        if (!string.Equals(functionName, CollectorContext.StatementsExtensionAuroraFunction, StringComparison.Ordinal))
        {
            return "pg_catalog";
        }

        try
        {
            var member = false;
            var found = new List<string>();
            using var command = connection.CreateCommand();
            command.CommandTimeout = DarlingCollectorRunner.PgExtensionSchemasTimeoutSeconds;
            command.CommandText = StatementsExtensionMemberSql;
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var schema = reader.GetString(0);
                    var arguments = reader.GetString(1);
                    var isMember = reader.GetBoolean(2);
                    found.Add($"{schema}.{functionName}({arguments}), member: {isMember}");
                    member |= isMember
                        && string.Equals(schema, statementsSchema, StringComparison.Ordinal)
                        && string.Equals(arguments, CollectorContext.StatementsExtensionAuroraFunctionArguments, StringComparison.Ordinal);
                }
            }

            if (member)
            {
                return statementsSchema;
            }

            logger?.LogDebug(
                "{Function}({Arguments}) is not a member of pg_stat_statements in schema {Schema}, so it is not used; found: {Found} (#6143)",
                functionName, CollectorContext.StatementsExtensionAuroraFunctionArguments, statementsSchema,
                found.Count == 0 ? "none" : string.Join("; ", found));
            return "pg_catalog";
        }
        catch (DbException ex)
        {
            logger?.LogDebug(ex, "Reading whether {Function} is a member of pg_stat_statements did not succeed (#6143)", functionName);
            return null;
        }
    }
}
