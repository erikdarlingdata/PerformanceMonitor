/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8320: prose the product writes into an answer the statement filter (#4348) sweeps must not be named by the
/// judge, or the reader gets the withheld marker where the explanation should be. Each test builds the real
/// message through its own entry point and asserts the judge hands back the same instance.
/// </summary>
public sealed class ProductProseIsNotNamedTests
{
    private static readonly JsonSerializerOptions s_json = new() { ReferenceHandler = ReferenceHandler.IgnoreCycles };

    /// <summary>The run note for user databases the monitoring login cannot enter is stored on the collection-log
    /// row and served by get_collection_log, so its Amazon RDS form must read too.</summary>
    [Fact]
    public void TheNoDatabaseAccessSentence_OnRds_IsNotNamed()
    {
        var sentence = NoDatabaseAccessNote.Sentence(new[] { "Sales", "Billing" }, awsRds: true)!;
        Assert.Same(sentence, SensitiveStatements.Text(sentence));
    }

    /// <summary>The connection test's warning for the same condition travels in the connect result, on Amazon RDS
    /// and elsewhere.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void TheConnectionTestNoUserWarning_IsNotNamed(bool awsRds)
    {
        var warning = MonitoringGrantScript.DescribeNoUserDatabases(3, "pm", awsRds)!;
        Assert.Same(warning, SensitiveStatements.Text(warning));
    }

    /// <summary>get_pg_server_config's note on what the database_overrides rows are.</summary>
    [Fact]
    public void TheDatabaseOverridesNote_IsNotNamed()
    {
        var note = DarlingMcpPgServerStateTools.DatabaseOverridesNote;
        Assert.Same(note, SensitiveStatements.Text(note));
    }

    /// <summary>get_pg_logging_audit's scope note for a value that came from a per-role override, built from a row
    /// whose source is 'user' through the audit itself.</summary>
    [Fact]
    public void TheLoggingAuditScopeNote_ForARoleOverride_IsNotNamed()
    {
        var row = JsonSerializer.Deserialize<DarlingPgLoggingAuditReader.PgLoggingSettingRow>(
            """{"Name":"log_min_duration_statement","Setting":"1000","Unit":"ms","Context":"superuser","Source":"user","BootValue":"-1"}""")!;

        using var audit = JsonDocument.Parse(JsonSerializer.Serialize(DarlingPgLoggingAudit.Audit(new[] { row }), s_json));
        var notes = Strings(audit.RootElement, "ScopeNote").ToList();

        Assert.NotEmpty(notes);
        Assert.All(notes, note =>
        {
            Assert.Contains("RESET", note, System.StringComparison.Ordinal);
            Assert.Same(note, SensitiveStatements.Text(note));
        });
    }

    /// <summary>The evict_cached_plan refusal when the remediation login lacks the permission, through the refusal
    /// builder the executor uses: every string in the answer reads.</summary>
    [Fact]
    public void TheEvictPermissionRefusal_IsNotNamed()
    {
        var refusal = SqlServerRemediationExecutor.EvictRefusal("evict_cached_plan", dryRun: true, edition: 3, granted: false, cached: 1,
            statement: PlanEvictStatement.For("0x06"))!;

        using var answer = JsonDocument.Parse(JsonSerializer.Serialize(refusal, s_json));
        var strings = Strings(answer.RootElement, null).ToList();

        Assert.Contains(strings, s => s.Contains("server-state permission", System.StringComparison.Ordinal));
        Assert.All(strings, s => Assert.Same(s, SensitiveStatements.Text(s)));
    }

    /// <summary>The Azure SQL Database kill_session refusal's tail when the login has no user in the session's database.</summary>
    [Fact]
    public void TheAzureKillNoUserHint_IsNotNamed()
    {
        var hint = SqlServerRemediationExecutor.NoUserInDatabaseHint;
        Assert.Same(hint, SensitiveStatements.Text(hint));
    }

    /// <summary>Every string value under <paramref name="element"/>, or only those whose property name is
    /// <paramref name="propertyName"/> when one is given.</summary>
    private static IEnumerable<string> Strings(JsonElement element, string? propertyName)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    if (property.Value.ValueKind == JsonValueKind.String
                        && (propertyName is null || property.Name == propertyName))
                    {
                        yield return property.Value.GetString()!;
                    }
                    else
                    {
                        foreach (var s in Strings(property.Value, propertyName)) yield return s;
                    }
                }
                break;
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    foreach (var s in Strings(item, propertyName)) yield return s;
                }
                break;
        }
    }
}
