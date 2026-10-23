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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Remediation;

/// <summary>A plan guide as <c>sys.sp_create_plan_guide</c> takes it (#6569, slice 2a): what a create makes, what its journal row holds, and what a re-create (slice 2b) reads back.</summary>
public sealed record PlanGuideDefinition(string Name, string Type, string Statement, string? ModuleOrBatch, string? Params, string Hints)
{
    /// <summary>The longest text of a statement, batch, parameter list or hint clause: a <c>USE PLAN</c> hint carries a whole plan, so the cap is generous and only stops a runaway argument.</summary>
    public const int MaxText = 4_000_000;

    /// <summary>The longest guide name.</summary>
    public const int MaxName = 128;

    private static readonly Regex OptionClause = new(@"^OPTION\s*\(", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// Checks the arguments of a create and returns the definition in its one normal form, or the refusal that names what is wrong. An OBJECT guide's module is
    /// normalized to <c>[schema].[name]</c> (<see cref="ModuleRecompileStatement.TryNormalize"/>) and takes no parameter list. A SQL guide's batch is optional (none means the
    /// statement is the batch). TEMPLATE is refused. Pure: nothing here reads a server.
    /// </summary>
    public static (PlanGuideDefinition? Definition, string? Code, string? Message) TryCreate(string? name, string? statement, string? type, string? moduleOrBatch, string? parameters, string? hints)
    {
        var trimmedName = name?.Trim();
        if (string.IsNullOrEmpty(trimmedName) || trimmedName.Length > MaxName || trimmedName[0] == '#' || HasControl(trimmedName))
            return (null, RemediationResult.Reasons.BadGuideName, "name is the plan guide's name: 1 to 128 characters, no control characters, and it does not start with #. Nothing was run.");

        var kind = type?.Trim().ToUpperInvariant();
        if (kind == "TEMPLATE")
            return (null, RemediationResult.Reasons.BadGuideType, "A TEMPLATE plan guide is not supported here: type is OBJECT or SQL. Nothing was run.");
        if (kind is not ("OBJECT" or "SQL"))
            return (null, RemediationResult.Reasons.BadGuideType, "type is OBJECT (a guide on a procedure, function or trigger) or SQL (a guide on a batch). Nothing was run.");

        if (string.IsNullOrWhiteSpace(statement) || statement.Length > MaxText)
            return (null, RemediationResult.Reasons.BadGuideArgument, "statement is the text of the statement the guide applies to, exactly as it is written in the module or batch. Nothing was run.");
        if (string.IsNullOrWhiteSpace(hints) || hints.Length > MaxText || !OptionClause.IsMatch(hints.Trim()))
            return (null, RemediationResult.Reasons.BadGuideArgument, "hints is the OPTION (...) clause to attach, for example OPTION (RECOMPILE) or OPTION (MAXDOP 1). Nothing was run.");
        if (parameters is { Length: > MaxText } || moduleOrBatch is { Length: > MaxText })
            return (null, RemediationResult.Reasons.BadGuideArgument, "module_or_batch or params is longer than a plan guide can take. Nothing was run.");

        string? target;
        string? declared;
        if (kind == "OBJECT")
        {
            if (!ModuleRecompileStatement.TryNormalize(moduleOrBatch, out var module))
                return (null, RemediationResult.Reasons.BadModuleName, "module_or_batch is the two-part schema.name of the procedure, function or trigger an OBJECT guide is on. Nothing was run.");
            if (!string.IsNullOrWhiteSpace(parameters))
                return (null, RemediationResult.Reasons.BadGuideArgument, "An OBJECT guide takes no params. Nothing was run.");
            target = module;
            declared = null;
        }
        else
        {
            target = string.IsNullOrWhiteSpace(moduleOrBatch) ? null : moduleOrBatch;
            declared = string.IsNullOrWhiteSpace(parameters) ? null : parameters;
        }

        return (new PlanGuideDefinition(trimmedName, kind, statement, target, declared, hints.Trim()), null, null);
    }

    private static bool HasControl(string text)
    {
        foreach (var c in text)
            if (char.IsControl(c))
                return true;
        return false;
    }
}

public sealed partial class SqlServerRemediationExecutor
{
    /// <summary>The <c>sys.objects.type</c> codes an OBJECT plan guide accepts: a stored procedure, a scalar function, a multi-statement table-valued function or a DML trigger.</summary>
    internal static readonly string[] PlanGuideModuleTypes = ["P", "FN", "TF", "TR"];

    /// <summary>
    /// The one batch of a plan guide create (#6569), run in the request's database. It reads what decides the create (whether the name is taken, whether an OBJECT guide's
    /// module resolves and holds the statement, the grants, how many cached batches equal a SQL guide's batch) and, only when <c>@run</c> is 1 and nothing refuses, runs
    /// <c>sys.sp_create_plan_guide</c>, then asks <c>sys.fn_validate_plan_guide</c> about the new guide and drops it in the same batch when that returns a row. Every value is a
    /// typed parameter; none joins the batch text. The cached-plan count needs <c>VIEW SERVER STATE</c>, so it sits in a TRY block: a login without it gets null, not an error.
    /// </summary>
    internal static readonly string CreatePlanGuideBatchSql = @"
DECLARE @exists bit = 0, @id integer = NULL, @otype varchar(2) = NULL, @module nvarchar(600) = NULL, @in_module bit = 0, @target nvarchar(max) = NULL,
        @can_db bigint = 0, @can_mod bigint = 1, @cached bigint = NULL, @ran bit = 0, @dropped bit = 0, @vmsg nvarchar(2048) = NULL, @gid integer = NULL;
IF EXISTS (SELECT 1 FROM sys.plan_guides AS pg WHERE pg.name = @name) SET @exists = 1;
SET @can_db = CONVERT(bigint, ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'ALTER'), 0));
IF @type = N'OBJECT'
BEGIN
    SET @id = OBJECT_ID(@mob);
    IF @id IS NOT NULL
    BEGIN
        SELECT @otype = RTRIM(o.type), @module = QUOTENAME(OBJECT_SCHEMA_NAME(o.object_id)) + N'.' + QUOTENAME(o.name)
        FROM sys.objects AS o
        WHERE o.object_id = @id;
        SET @can_mod = CONVERT(bigint, ISNULL(HAS_PERMS_BY_NAME(@module, N'OBJECT', N'ALTER'), 0));
        IF CHARINDEX(@stmt COLLATE Latin1_General_BIN2, OBJECT_DEFINITION(@id) COLLATE Latin1_General_BIN2) > 0 SET @in_module = 1;
    END;
    SET @target = @module;
END
ELSE
BEGIN
    SET @target = @mob;
    BEGIN TRY
        SELECT @cached = COUNT_BIG(*)
        FROM sys.dm_exec_cached_plans AS cp
        CROSS APPLY sys.dm_exec_sql_text(cp.plan_handle) AS st
        WHERE st.text COLLATE Latin1_General_BIN2 = ISNULL(@mob, @stmt) COLLATE Latin1_General_BIN2;
    END TRY
    BEGIN CATCH
        SET @cached = NULL;
    END CATCH;
END;
IF @run = 1 AND @exists = 0 AND @can_db = 1 AND @can_mod = 1
   AND (@type = N'SQL' OR (@id IS NOT NULL AND @otype IN (" + string.Join(", ", Array.ConvertAll(PlanGuideModuleTypes, t => "N'" + t + "'")) + @") AND @in_module = 1))
BEGIN
    EXEC sys.sp_create_plan_guide @name = @name, @stmt = @stmt, @type = @type, @module_or_batch = @target, @params = @params, @hints = @hints;
    SELECT @gid = pg.plan_guide_id FROM sys.plan_guides AS pg WHERE pg.name = @name;
    SELECT TOP (1) @vmsg = v.message FROM sys.fn_validate_plan_guide(@gid) AS v;
    IF @vmsg IS NOT NULL
    BEGIN
        EXEC sys.sp_control_plan_guide N'DROP', @name;
        SET @dropped = 1;
    END
    ELSE SET @ran = 1;
END;
SELECT name_taken = CONVERT(bigint, @exists), module_found = CASE WHEN @id IS NULL THEN CONVERT(bigint, 0) ELSE CONVERT(bigint, 1) END, module_type = @otype,
       statement_in_module = CONVERT(bigint, @in_module), can_db = @can_db, can_module = @can_mod, cached_matches = @cached,
       ran = CONVERT(bigint, @ran), dropped = CONVERT(bigint, @dropped), validation = @vmsg;";

    /// <summary>
    /// The one batch of a plan guide drop (#6569), run in the request's database: it reads the guide by name (its scope, its statement and hints), the grants, and, when the
    /// caller names the definition it expects, whether the guide still holds that statement and those hints, and, only when <c>@run</c> is 1 and nothing refuses, runs
    /// <c>sys.sp_control_plan_guide N'DROP'</c>. A guide somebody replaced under the same name does not match and is not dropped.
    /// </summary>
    internal static readonly string DropPlanGuideBatchSql = @"
DECLARE @found bit = 0, @scope integer = NULL, @scope_obj integer = NULL, @stmt_db nvarchar(max) = NULL, @hints_db nvarchar(max) = NULL, @module nvarchar(600) = NULL,
        @same bit = 1, @can_db bigint = 0, @can_mod bigint = 1, @ran bit = 0;
SELECT @found = 1, @scope = pg.scope_type, @scope_obj = pg.scope_object_id, @stmt_db = pg.query_text, @hints_db = pg.hints
FROM sys.plan_guides AS pg
WHERE pg.name = @name;
IF @found = 1
BEGIN
    SET @can_db = CONVERT(bigint, ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'ALTER'), 0));
    IF @scope = 1 AND @scope_obj IS NOT NULL
    BEGIN
        SET @module = QUOTENAME(OBJECT_SCHEMA_NAME(@scope_obj)) + N'.' + QUOTENAME(OBJECT_NAME(@scope_obj));
        SET @can_mod = CONVERT(bigint, ISNULL(HAS_PERMS_BY_NAME(@module, N'OBJECT', N'ALTER'), 0));
    END;
    IF @expect_stmt IS NOT NULL AND (@stmt_db COLLATE Latin1_General_BIN2 <> @expect_stmt COLLATE Latin1_General_BIN2 OR @hints_db COLLATE Latin1_General_BIN2 <> @expect_hints COLLATE Latin1_General_BIN2) SET @same = 0;
END;
IF @run = 1 AND @found = 1 AND @same = 1 AND @can_db = 1 AND @can_mod = 1
BEGIN
    EXEC sys.sp_control_plan_guide N'DROP', @name;
    SET @ran = 1;
END;
SELECT found = CONVERT(bigint, @found), same = CONVERT(bigint, @same), can_db = @can_db, can_module = @can_mod, ran = CONVERT(bigint, @ran);";

    /// <summary>
    /// Creates ONE plan guide with <c>sys.sp_create_plan_guide</c> in the permit's database (#6569, slice 2a), after the checks the batch gathers, and validates it before it
    /// stays: a guide <c>sys.fn_validate_plan_guide</c> reports on is dropped in the same batch and the result is <c>failed</c> with <c>guide_invalid</c> and the message the
    /// server gave. A refused call changes nothing. A dry run reads the same facts and writes nothing. <see cref="RemediationResult.CachedMatches"/> is the count of cached
    /// batches equal to a SQL guide's batch (null for an OBJECT guide, or when the login cannot read the cache). The permit's id is the guide's name.
    /// </summary>
    public async Task<RemediationResult> CreatePlanGuideAsync(ServerActionPermit permit, PlanGuideDefinition guide, bool dryRun, CancellationToken ct)
    {
        const string name = "create_plan_guide";
        ArgumentNullException.ThrowIfNull(guide);
        if (PermitRefusal(permit, ServerActionNames.CreatePlanGuide, guide.Name, name, dryRun) is { } mismatch)
            return mismatch;
        var database = permit.Database;
        if (string.IsNullOrWhiteSpace(database) || database.Length > 128 || ContainsControl(database))
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadDatabase,
                "A plan guide is created in one named database. The permit names none, or names one that is not a database name."));
        var statement = PlanGuideStatement.Create(database, guide);

        return await RunAsync(name, dryRun, database, async (connection, secret) =>
        {
            bool taken, found, inModule, ran, dropped;
            string? type, validation;
            bool grantedDb, grantedModule;
            long? cached;
            await using var batch = Command(connection, CreatePlanGuideBatchSql);
            batch.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 128) { Value = guide.Name });
            batch.Parameters.Add(new SqlParameter("@stmt", SqlDbType.NVarChar, -1) { Value = guide.Statement });
            batch.Parameters.Add(new SqlParameter("@type", SqlDbType.NVarChar, 60) { Value = guide.Type });
            batch.Parameters.Add(new SqlParameter("@mob", SqlDbType.NVarChar, -1) { Value = (object?)guide.ModuleOrBatch ?? DBNull.Value });
            batch.Parameters.Add(new SqlParameter("@params", SqlDbType.NVarChar, -1) { Value = (object?)guide.Params ?? DBNull.Value });
            batch.Parameters.Add(new SqlParameter("@hints", SqlDbType.NVarChar, -1) { Value = guide.Hints });
            batch.Parameters.Add(new SqlParameter("@run", SqlDbType.Bit) { Value = !dryRun });
            try
            {
                await using var reader = await batch.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct))
                    return RemediationResult.Failed(name, dryRun, "The server returned no answer to the plan guide check.");
                taken = Flag(reader, 0);
                found = Flag(reader, 1);
                type = reader.IsDBNull(2) ? null : reader.GetString(2);
                inModule = Flag(reader, 3);
                grantedDb = Flag(reader, 4);
                grantedModule = Flag(reader, 5);
                cached = reader.IsDBNull(6) ? null : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture);
                ran = Flag(reader, 7);
                dropped = Flag(reader, 8);
                validation = reader.IsDBNull(9) ? null : reader.GetString(9);
            }
            catch (Exception ex) when (!dryRun && (ex is OperationCanceledException || IsOutcomeUnknown(ex, connection.State == ConnectionState.Open, ct.IsCancellationRequested)))
            {
                var detail = ex is OperationCanceledException ? "the call was cancelled while the server ran it" : ErrorText(ex, secret);
                return RemediationResult.Unknown(name,
                    $"The create of plan guide {guide.Name} in {database} was sent, but its reply was lost ({detail}). The guide may exist: read sys.plan_guides before you try again.");
            }

            if (CreatePlanGuideRefusal(name, dryRun, guide, database, taken, found, type, inModule, grantedDb, grantedModule, statement) is { } refusal)
                return refusal;

            var matches = guide.Type == "SQL" ? cached : null;
            if (dryRun)
                return RemediationResult.Done(name, dryRun, wrote: false, $"Dry run: plan guide {guide.Name} would be created in {database}. {MatchWords(guide, matches)} Nothing was run.")
                    with { Statement = statement, CachedMatches = matches };
            if (dropped)
                return RemediationResult.Failed(name, dryRun,
                    $"The server created plan guide {guide.Name} in {database}, found it invalid, and the guide was dropped again: {validation}", RemediationResult.Reasons.GuideInvalid)
                    with { Statement = statement, CachedMatches = matches, Validation = validation };
            if (!ran)
                return RemediationResult.Failed(name, dryRun, $"The server did not create plan guide {guide.Name} in {database}.") with { Statement = statement };

            return RemediationResult.Done(name, dryRun, wrote: true, $"Plan guide {guide.Name} was created in {database} and passed validation. {MatchWords(guide, matches)}")
                with { Statement = statement, CachedMatches = matches };
        }, ct);
    }

    /// <summary>What the create says about the plans a guide would apply to. An OBJECT guide applies to the module's own statement; a SQL guide to cached or future batches that equal its batch.</summary>
    internal static string MatchWords(PlanGuideDefinition guide, long? matches) =>
        guide.Type == "OBJECT" ? "The statement was found in the module's definition."
        : matches is null ? "Darling could not count the cached batches that match (the login cannot read the plan cache)."
        : matches == 0 ? "No cached batch matches this guide right now, so it applies to nothing cached yet; it applies when a batch equal to its text next compiles."
        : string.Create(CultureInfo.InvariantCulture, $"{matches} cached batch{(matches == 1 ? "" : "es")} equal this guide's text right now.");

    /// <summary>
    /// The only refusals of a plan guide create, in the order the batch gathered their facts: the name is taken, an OBJECT guide's module does not resolve, is not a procedure,
    /// function or trigger, does not hold the statement, or the login lacks <c>ALTER</c>. Null when none applies. Pure, so a test needs no server.
    /// </summary>
    internal static RemediationResult? CreatePlanGuideRefusal(string name, bool dryRun, PlanGuideDefinition guide, string database, bool taken, bool moduleFound, string? moduleType,
        bool statementInModule, bool grantedDb, bool grantedModule, string statement)
    {
        if (taken)
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.GuideExists,
                $"A plan guide named {guide.Name} already exists in {database}. Pick another name. Nothing was run.") with { Statement = statement };
        if (guide.Type == "OBJECT")
        {
            if (!moduleFound)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.ModuleNotFound,
                    $"{guide.ModuleOrBatch} does not resolve to an object in {database}. Check the database and the schema. Nothing was run.") with { Statement = statement };
            if (moduleType is null || Array.IndexOf(PlanGuideModuleTypes, moduleType) < 0)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.NotAModule,
                    $"{guide.ModuleOrBatch} is not a stored procedure, scalar function, multi-statement table-valued function or DML trigger, the objects an OBJECT plan guide can be on. Nothing was run.") with { Statement = statement };
            if (!statementInModule)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.StatementNotInModule,
                    $"The statement does not appear exactly (character for character, case and spacing included) in the definition of {guide.ModuleOrBatch}. Copy it from OBJECT_DEFINITION. Nothing was run.") with { Statement = statement };
        }

        if (!grantedDb || !grantedModule)
            return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.GuidePermissionDenied,
                $"The remediation login does not hold ALTER on {database}{(guide.Type == "OBJECT" ? $" and on {guide.ModuleOrBatch}" : "")}, which sp_create_plan_guide needs. Nothing was run.") with { Statement = statement };
        return null;
    }

    /// <summary>
    /// Drops ONE plan guide by name with <c>sys.sp_control_plan_guide N'DROP'</c> (#6569), in the permit's database. Used by the undo of a create (the permit is the create switch's)
    /// and by the drop action (slice 2b, the drop switch's). With <paramref name="expected"/> it drops only a guide that still holds that statement and those hints (<c>guide_changed</c>
    /// otherwise), so an undo never drops a guide somebody made under the same name since. A guide that is not there is <c>guide_not_found</c>: nothing to drop.
    /// </summary>
    public async Task<RemediationResult> DropPlanGuideAsync(ServerActionPermit permit, string guideName, PlanGuideDefinition? expected, bool dryRun, CancellationToken ct)
    {
        const string name = "drop_plan_guide";
        var action = permit?.ServerAction == ServerActionNames.DropPlanGuide ? ServerActionNames.DropPlanGuide : ServerActionNames.CreatePlanGuide;
        if (PermitRefusal(permit, action, guideName, name, dryRun) is { } mismatch)
            return mismatch;
        var database = permit!.Database;
        if (string.IsNullOrWhiteSpace(database) || database.Length > 128 || ContainsControl(database))
            return Log(RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.BadDatabase,
                "A plan guide is dropped in one named database. The permit names none, or names one that is not a database name."));
        var statement = PlanGuideStatement.Drop(database, guideName);

        return await RunAsync(name, dryRun, database, async (connection, secret) =>
        {
            bool found, same, grantedDb, grantedModule, ran;
            await using var batch = Command(connection, DropPlanGuideBatchSql);
            batch.Parameters.Add(new SqlParameter("@name", SqlDbType.NVarChar, 128) { Value = guideName });
            batch.Parameters.Add(new SqlParameter("@expect_stmt", SqlDbType.NVarChar, -1) { Value = (object?)expected?.Statement ?? DBNull.Value });
            batch.Parameters.Add(new SqlParameter("@expect_hints", SqlDbType.NVarChar, -1) { Value = (object?)expected?.Hints ?? DBNull.Value });
            batch.Parameters.Add(new SqlParameter("@run", SqlDbType.Bit) { Value = !dryRun });
            try
            {
                await using var reader = await batch.ExecuteReaderAsync(ct);
                if (!await reader.ReadAsync(ct))
                    return RemediationResult.Failed(name, dryRun, "The server returned no answer to the plan guide check.");
                found = Flag(reader, 0);
                same = Flag(reader, 1);
                grantedDb = Flag(reader, 2);
                grantedModule = Flag(reader, 3);
                ran = Flag(reader, 4);
            }
            catch (Exception ex) when (!dryRun && (ex is OperationCanceledException || IsOutcomeUnknown(ex, connection.State == ConnectionState.Open, ct.IsCancellationRequested)))
            {
                var detail = ex is OperationCanceledException ? "the call was cancelled while the server ran it" : ErrorText(ex, secret);
                return RemediationResult.Unknown(name,
                    $"The drop of plan guide {guideName} in {database} was sent, but its reply was lost ({detail}). The guide may be gone: read sys.plan_guides before you try again.");
            }

            if (!found)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.GuideNotFound,
                    $"There is no plan guide named {guideName} in {database}: it is already gone. Nothing was changed.") with { Statement = statement };
            if (!same)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.GuideChanged,
                    $"Plan guide {guideName} in {database} no longer holds the statement and hints it was created with, so it was changed or replaced since. Nothing was changed.") with { Statement = statement };
            if (!grantedDb || !grantedModule)
                return RemediationResult.Refused(name, dryRun, RemediationResult.Reasons.GuidePermissionDenied,
                    $"The remediation login does not hold ALTER on {database} (and on the guide's module, for an OBJECT guide), which sp_control_plan_guide needs. Nothing was changed.") with { Statement = statement };
            if (dryRun)
                return RemediationResult.Done(name, dryRun, wrote: false, $"Dry run: plan guide {guideName} would be dropped from {database}. Nothing was run.") with { Statement = statement };
            if (!ran)
                return RemediationResult.Failed(name, dryRun, $"The server did not drop plan guide {guideName} in {database}.") with { Statement = statement };
            return RemediationResult.Done(name, dryRun, wrote: true, $"Plan guide {guideName} was dropped from {database}.") with { Statement = statement };
        }, ct);
    }

    private static bool Flag(SqlDataReader reader, int ordinal) =>
        !reader.IsDBNull(ordinal) && Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture) == 1;
}

/// <summary>The exact statements a plan guide create and drop run (#6569). The statement text lives in this assembly only; the executor itself sends every value as a typed parameter.</summary>
public static class PlanGuideStatement
{
    /// <summary>What a person would run in <paramref name="database"/> to make the guide.</summary>
    public static string Create(string database, PlanGuideDefinition guide) =>
        "USE " + QuoteName(database) + "; EXEC sys.sp_create_plan_guide @name = " + Literal(guide.Name) + ", @stmt = " + Literal(guide.Statement) + ", @type = " + Literal(guide.Type)
        + ", @module_or_batch = " + Literal(guide.ModuleOrBatch) + ", @params = " + Literal(guide.Params) + ", @hints = " + Literal(guide.Hints) + ";";

    /// <summary>What a person would run in <paramref name="database"/> to drop the guide.</summary>
    public static string Drop(string database, string guideName) =>
        "USE " + QuoteName(database) + "; EXEC sys.sp_control_plan_guide N'DROP', " + Literal(guideName) + ";";

    private static string Literal(string? text) => text is null ? "NULL" : "N'" + text.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static string QuoteName(string name) => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";
}
