/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Remediation;

public sealed partial class SqlServerRemediationExecutor
{
    /// <summary>The <c>sys.objects.type</c> codes <c>recompile_module</c> accepts: stored procedures (T-SQL and CLR), scalar, inline and table-valued functions (T-SQL and CLR) and DML triggers.</summary>
    internal static readonly string[] RecompilableTypes = ["P", "PC", "FN", "IF", "TF", "FS", "FT", "TR"];

    /// <summary>
    /// The one batch of a module recompile (#6569), run in the request's database: it resolves the name to an object id, reads what that object is and
    /// whether the login holds <c>ALTER</c> on it, and, only when <c>@run</c> is 1 and the object is an accepted module the login may alter, runs
    /// <c>sys.sp_recompile</c> on that same object's schema and name. It answers which object it resolved and whether the recompile ran, so the check
    /// and the recompile cannot see two different objects. The name goes in as <c>@p0</c>, a typed parameter, and never joins the batch text.
    /// </summary>
    internal static readonly string RecompileBatchSql = @"
DECLARE @id integer = OBJECT_ID(@p0);
DECLARE @type varchar(2), @type_desc nvarchar(60), @schema sysname, @name sysname, @resolved nvarchar(600), @can_alter bigint = 0, @ran bit = 0;
SELECT @type = RTRIM(o.type), @type_desc = o.type_desc, @schema = OBJECT_SCHEMA_NAME(o.object_id), @name = o.name
FROM sys.objects AS o
WHERE o.object_id = @id;
IF @id IS NOT NULL
BEGIN
    SET @resolved = QUOTENAME(@schema) + N'.' + QUOTENAME(@name);
    SET @can_alter = CONVERT(bigint, ISNULL(HAS_PERMS_BY_NAME(@resolved, N'OBJECT', N'ALTER'), 0));
END;
IF @run = 1 AND @id IS NOT NULL AND @can_alter = 1 AND @type IN (" + string.Join(", ", Array.ConvertAll(RecompilableTypes, t => "N'" + t + "'")) + @")
BEGIN
    EXEC sys.sp_recompile @objname = @resolved;
    SET @ran = 1;
END;
SELECT found = CASE WHEN @id IS NULL THEN 0 ELSE 1 END, type = @type, type_desc = @type_desc, can_alter = @can_alter,
       schema_name = @schema, object_name = @name, ran = CONVERT(bigint, @ran);";

    /// <summary>
    /// Recompiles ONE procedure, function or trigger with <c>sys.sp_recompile</c> (#6569), so the next run compiles a new plan. The permit names the
    /// database and the object (<see cref="ModuleRecompileStatement.TryNormalize"/>'s form). One batch, in that database, gathers whether the name
    /// resolves (<c>module_not_found</c>), whether it is a procedure, function or trigger (<c>not_a_module</c>: a table or a view is refused) and whether the
    /// login holds <c>ALTER</c> on it (<c>recompile_permission_denied</c>). Those are the only refusals. SQL Server and Azure SQL Database both have the procedure.
    /// </summary>
    public async Task<RemediationResult> RecompileModuleAsync(ServerActionPermit permit, string moduleName, bool dryRun, CancellationToken ct)
    {
        const string name = "recompile_module";
        if (!ModuleRecompileStatement.TryNormalize(moduleName, out var normal))
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadModuleName,
                "A module is named schema.name, two parts, each at most 128 characters. Nothing was run."));
        if (PermitRefusal(permit, ServerActionNames.RecompileModule, normal, name, dryRun) is { } mismatch)
            return mismatch;
        var database = permit.Database;
        if (string.IsNullOrWhiteSpace(database) || database.Length > 128 || ContainsControl(database))
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadDatabase,
                "A module is recompiled in one named database. The permit names none, or names one that is not a database name."));
        var statement = ModuleRecompileStatement.For(database, normal);

        return await RunAsync(name, dryRun, database, async (connection, secret) =>
        {
            bool found;
            string? type;
            string? typeDesc;
            bool granted;
            string? resolvedSchema;
            string? resolvedName;
            bool ran;
            await using var batch = Command(connection, RecompileBatchSql);
            batch.Parameters.Add(new SqlParameter("@p0", SqlDbType.NVarChar, ModuleRecompileStatement.MaxNormalLength) { Value = normal });
            batch.Parameters.Add(new SqlParameter("@run", SqlDbType.Bit) { Value = !dryRun });
            try
            {
                await using var reader = await batch.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct))
                    return RemediationResult.Failed(name, dryRun, "The server returned no answer to the module check.");
                found = Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture) == 1;
                type = reader.IsDBNull(1) ? null : reader.GetString(1);
                typeDesc = reader.IsDBNull(2) ? null : reader.GetString(2);
                granted = !reader.IsDBNull(3) && Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture) == 1;
                resolvedSchema = reader.IsDBNull(4) ? null : reader.GetString(4);
                resolvedName = reader.IsDBNull(5) ? null : reader.GetString(5);
                ran = !reader.IsDBNull(6) && Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture) == 1;
            }
            catch (Exception ex) when (!dryRun && (ex is OperationCanceledException || IsOutcomeUnknown(ex, connection.State == ConnectionState.Open, ct.IsCancellationRequested)))
            {
                /* The batch was sent and its reply was lost, or the call was cancelled while it ran: the server may have recompiled it (#5632). */
                var detail = ex is OperationCanceledException ? "the call was cancelled while the server ran it" : ErrorText(ex, secret);
                return RemediationResult.Unknown(name,
                    $"The recompile of {normal} in {database} was sent, but its reply was lost ({detail}). The module may have been marked for recompile. Running it again does no harm.");
            }

            if (RecompileRefusal(name, dryRun, normal, database, found, type, typeDesc, granted, statement) is { } refusal)
                return refusal;

            /* The object the server resolved, not the text the caller typed (the two can differ in case). */
            var resolved = "[" + (resolvedSchema ?? "").Replace("]", "]]", StringComparison.Ordinal) + "].[" + (resolvedName ?? "").Replace("]", "]]", StringComparison.Ordinal) + "]";
            if (dryRun)
                return RemediationResult.Done(name, dryRun, wrote: false, $"Dry run: {resolved} is a {typeDesc} in {database}. Nothing was run.")
                    with { Statement = statement, ObjectType = typeDesc, ObjectSchema = resolvedSchema, ObjectName = resolvedName };
            if (!ran)
                return RemediationResult.Failed(name, dryRun, $"The server did not recompile {resolved} in {database}.");

            return RemediationResult.Done(name, dryRun, wrote: true, $"{resolved} was marked for recompile in {database}.")
                with { Statement = statement, ObjectType = typeDesc, ObjectSchema = resolvedSchema, ObjectName = resolvedName };
        }, ct);
    }

    /// <summary>
    /// The only refusals of a module recompile, in the order the read batch gathered their facts: the name does not resolve in the database, it
    /// resolves to something that is not a procedure, function or trigger, the login lacks <c>ALTER</c> on it. Null when none applies. Pure, so a test needs no server.
    /// </summary>
    internal static RemediationResult? RecompileRefusal(string name, bool dryRun, string module, string database, bool found, string? type, string? typeDesc,
        bool granted, string statement)
    {
        if (!found)
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.ModuleNotFound,
                $"{module} does not resolve to an object in {database}. Check the database and the schema. Nothing was run.") with { Statement = statement };
        if (type is null || Array.IndexOf(RecompilableTypes, type) < 0)
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotAModule,
                $"{module} is a {typeDesc ?? "different kind of object"}, not a procedure, function or trigger. Nothing was run.") with { Statement = statement, ObjectType = typeDesc };
        if (!granted)
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.RecompilePermissionDenied,
                $"The remediation login does not hold ALTER on {module} in {database}, which sp_recompile needs. A procedure or function needs ALTER on the module; a DML trigger needs ALTER on its table, which also allows changing and emptying that table, so a person should grant it only where that is acceptable. Nothing was run.")
                with { Statement = statement, ObjectType = typeDesc };
        return null;
    }
}

/// <summary>The one normal form of a module name, and the exact statement a recompile runs (#6569). The statement text lives in this assembly only.</summary>
public static class ModuleRecompileStatement
{
    private const int MaxPart = 128;

    /// <summary>The longest normal form: two parts of 128 characters, each all <c>]</c> doubled, plus two pairs of brackets and the dot.</summary>
    public const int MaxNormalLength = 2 * (2 * MaxPart + 2) + 1;

    /// <summary>
    /// The name as <c>[schema].[name]</c> (each part bracket-quoted, a <c>]</c> doubled), or false when it is not exactly two non-empty parts of at most
    /// 128 characters. Each part is plain text up to a dot, or bracketed (<c>[a.b]</c>, <c>]]</c> for a <c>]</c>). Control characters are refused. The
    /// result still goes to the server as a parameter, never as batch text.
    /// </summary>
    public static bool TryNormalize(string? name, out string normal)
    {
        normal = "";
        if (string.IsNullOrWhiteSpace(name))
            return false;
        var text = name.Trim();
        var parts = new System.Collections.Generic.List<string>(2);
        var i = 0;
        while (true)
        {
            var part = new StringBuilder();
            if (i < text.Length && text[i] == '[')
            {
                i++;
                var closed = false;
                while (i < text.Length)
                {
                    if (text[i] == ']')
                    {
                        if (i + 1 < text.Length && text[i + 1] == ']')
                        {
                            part.Append(']');
                            i += 2;
                            continue;
                        }

                        closed = true;
                        i++;
                        break;
                    }

                    part.Append(text[i++]);
                }

                if (!closed)
                    return false;
            }
            else
            {
                while (i < text.Length && text[i] != '.')
                    part.Append(text[i++]);
            }

            var value = part.ToString();
            if (value.Length is 0 or > MaxPart || value.Trim().Length == 0)
                return false;
            foreach (var c in value)
                if (char.IsControl(c))
                    return false;
            parts.Add(value);
            if (i >= text.Length)
                break;
            if (text[i] != '.')
                return false;
            i++;
        }

        if (parts.Count != 2)
            return false;
        normal = "[" + parts[0].Replace("]", "]]", StringComparison.Ordinal) + "].[" + parts[1].Replace("]", "]]", StringComparison.Ordinal) + "]";
        return true;
    }

    /// <summary>The statement a person would run in <paramref name="database"/>. <paramref name="normalName"/> is the output of <see cref="TryNormalize"/>; the executor itself sends the name as a typed parameter.</summary>
    public static string For(string database, string normalName)
    {
        if (!TryNormalize(normalName, out var normal))
            throw new ArgumentException("Not a module name.", nameof(normalName));
        return "USE " + QuoteName(database) + "; EXEC sys.sp_recompile @objname = N'" + normal.Replace("'", "''", StringComparison.Ordinal) + "';";
    }

    private static string QuoteName(string name) => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";
}
