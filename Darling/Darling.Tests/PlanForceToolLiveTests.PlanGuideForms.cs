/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #6569 guide forms: the plan guide tools take every form sp_create_plan_guide takes (OPTION hints, an XML showplan, no hints, and a TEMPLATE guide), so a dropped guide can always be
   undone. The same store and SQL Server setup as the other plan guide live tests (a partial class). */
public sealed partial class PlanForceToolLiveTests
{
    /* No literal in the text: a simple-parameterized statement is cached with a parameter list, so its cached text is not the text a guide matches. */
    private const string XmlGuideStatement = "SELECT a, b FROM dbo.t ORDER BY b DESC";

    /// <summary>Runs <paramref name="statement"/> so a plan for it is cached, then reads that plan as XML from the plan cache.</summary>
    private static async Task<string> CachedShowplanAsync(Ctx c, string statement)
    {
        /* Run it more than once: with "optimize for ad hoc workloads" on, the first run caches only a stub with no plan. */
        for (var i = 0; i < 3; i++)
            await AdminExecAsync(c, statement);
        await using var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
        await using var command = new Microsoft.Data.SqlClient.SqlCommand(
            "SELECT TOP (1) CONVERT(nvarchar(max), qp.query_plan) FROM sys.dm_exec_query_stats AS qs CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st "
            + "CROSS APPLY sys.dm_exec_text_query_plan(qs.plan_handle, DEFAULT, DEFAULT) AS qp WHERE st.text = @text AND CHARINDEX(QUOTENAME(DB_NAME()), qp.query_plan) > 0;", admin);
        command.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@text", System.Data.SqlDbType.NVarChar, -1) { Value = statement });
        var plan = await command.ExecuteScalarAsync(c.Ct);
        Assert.False(plan is null or DBNull, "the plan for the statement was not in the plan cache");
        return Convert.ToString(plan, CultureInfo.InvariantCulture)!;
    }

    /// <summary>The parameterized form of <paramref name="statement"/> and its parameter list, from <c>sys.sp_get_query_template</c>.</summary>
    private static async Task<(string Statement, string Params)> QueryTemplateAsync(Ctx c, string statement)
    {
        await using var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
        await using var command = new Microsoft.Data.SqlClient.SqlCommand(
            "DECLARE @s nvarchar(max), @p nvarchar(max); EXEC sys.sp_get_query_template @querytext, @s OUTPUT, @p OUTPUT; SELECT @s, @p;", admin);
        command.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@querytext", System.Data.SqlDbType.NVarChar, -1) { Value = statement });
        await using var reader = await command.ExecuteReaderAsync(c.Ct);
        Assert.True(await reader.ReadAsync(c.Ct));
        return (reader.GetString(0), reader.GetString(1));
    }

    [Fact]
    public async Task AGuideWithXmlShowplanHints_NullHints_OrOfTypeTemplate_IsDropped_AndItsUndoRebuildsTheSameDefinition()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var suffix = Guid.NewGuid().ToString("N")[..8];
            var xmlGuide = "x_" + suffix;
            var nullGuide = "n_" + suffix;
            var templateGuide = "t_" + suffix;
            var xmlStatement = XmlGuideStatement;
            var nullStatement = "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 2";
            var (templateStatement, templateParams) = await QueryTemplateAsync(c, "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 3");
            var xml = await CachedShowplanAsync(c, xmlStatement);
            await TurnDropOnAsync(c);

            await using (var admin = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
            {
                await using var create = new Microsoft.Data.SqlClient.SqlCommand(
                    "EXEC sys.sp_create_plan_guide @name = @xn, @stmt = @xs, @type = N'SQL', @module_or_batch = NULL, @params = NULL, @hints = @xh; "
                    + "EXEC sys.sp_create_plan_guide @name = @nn, @stmt = @ns, @type = N'SQL', @module_or_batch = NULL, @params = NULL, @hints = NULL; "
                    + "EXEC sys.sp_create_plan_guide @name = @tn, @stmt = @ts, @type = N'TEMPLATE', @module_or_batch = NULL, @params = @tp, @hints = N'OPTION (PARAMETERIZATION FORCED)';", admin);
                create.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@xn", System.Data.SqlDbType.NVarChar, -1) { Value = xmlGuide });
                create.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@xs", System.Data.SqlDbType.NVarChar, -1) { Value = xmlStatement });
                create.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@xh", System.Data.SqlDbType.NVarChar, -1) { Value = xml });
                create.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@nn", System.Data.SqlDbType.NVarChar, -1) { Value = nullGuide });
                create.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@ns", System.Data.SqlDbType.NVarChar, -1) { Value = nullStatement });
                create.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@tn", System.Data.SqlDbType.NVarChar, -1) { Value = templateGuide });
                create.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@ts", System.Data.SqlDbType.NVarChar, -1) { Value = templateStatement });
                create.Parameters.Add(new Microsoft.Data.SqlClient.SqlParameter("@tp", System.Data.SqlDbType.NVarChar, -1) { Value = templateParams });
                await create.ExecuteNonQueryAsync(c.Ct);
            }

            foreach (var (guide, type, hasHints) in new[] { (xmlGuide, "SQL", true), (nullGuide, "SQL", false), (templateGuide, "TEMPLATE", true) })
            {
                var original = await GuideRowAsync(c, guide);
                Assert.NotNull(original);
                Assert.Equal(hasHints, await GuideColumnAsync(c, guide, "hints") is not null);

                /* The dry run reads the whole definition: NULL hints stay NULL, and the type is the guide's own. */
                var dry = Parse(await DropGuideAsync(c, guide, dryRun: true));
                Assert.Equal("dry_run", (string?)dry["status"]);
                var image = dry["before"]!["guide"]!;
                Assert.Equal(type, (string?)image["type"]);
                Assert.Equal(hasHints, image["hints"] is not null);
                Assert.Equal(original, await GuideRowAsync(c, guide));

                var dropped = await RealDropAsync(c, guide);
                Assert.Null(await GuideRowAsync(c, guide));

                var undone = await UndoAsync(c, dropped, confirm: true);
                Assert.Equal(PlanUndoAnswer.Undone, undone.Status);
                Assert.Equal(original, await GuideRowAsync(c, guide));
            }
        });
    }

    [Fact]
    public async Task ACreateWithXmlShowplanHints_NullHints_OrOfTypeTemplate_IsUndoneByDroppingTheGuide()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var suffix = Guid.NewGuid().ToString("N")[..8];
            await TurnGuideOnAsync(c);
            var xml = await CachedShowplanAsync(c, XmlGuideStatement);
            var (templateStatement, templateParams) = await QueryTemplateAsync(c, "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 3");

            var cases = new (string Guide, string Type, string Statement, string? Params, string? Hints)[]
            {
                ("x_" + suffix, "SQL", XmlGuideStatement, null, xml),
                ("n_" + suffix, "SQL", "SELECT n = COUNT_BIG(*) FROM dbo.t WHERE a >= 2", null, null),
                ("t_" + suffix, "TEMPLATE", templateStatement, templateParams, "OPTION (PARAMETERIZATION FORCED)"),
            };
            foreach (var (guide, type, statement, parameters, hints) in cases)
            {
                var dry = Parse(await CreateGuideAsync(c, guide, dryRun: true, type: type, statement: statement, hints: hints, parameters: parameters));
                Assert.Equal("dry_run", (string?)dry["status"]);
                if (type == "TEMPLATE")
                    Assert.Null(dry["matches"]);

                var created = Parse(await CreateGuideAsync(c, guide, dryRun: false, type: type, statement: statement, hints: hints, parameters: parameters));
                Assert.True((string?)created["status"] == "done", (string?)created["message"]);
                Assert.Equal(hints is null, await GuideColumnAsync(c, guide, "hints") is null);
                Assert.Equal(type == "TEMPLATE" ? "3" : "2", await GuideColumnAsync(c, guide, "scope_type"));

                var undone = await UndoAsync(c, (long)created["journal_id"]!, confirm: true);
                Assert.True(undone.Status == PlanUndoAnswer.Undone, undone.Message);
                Assert.Null(await GuideRowAsync(c, guide));
            }
        });
    }
}
