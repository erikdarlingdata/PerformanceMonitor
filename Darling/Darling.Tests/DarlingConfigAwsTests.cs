/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The per-server AWS role in darling.json (#5452): the two fields, the shared key, and the checks
/// <see cref="DarlingConfig.Validate"/> runs on them.
/// </summary>
public sealed class DarlingConfigAwsTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";

    private static MonitoredServer PgServer(string? role, string? externalId) => new()
    {
        Name = "aurora-writer",
        Engine = "postgres",
        Host = "aurora.cluster-example.us-east-1.rds.amazonaws.com",
        Auth = "sql",
        Username = "monitor",
        Password = "env:PGPASSWORD",
        AwsRoleArn = role,
        AwsExternalId = externalId,
    };

    private static DarlingConfig ConfigWith(MonitoredServer server)
    {
        var config = new DarlingConfig();
        config.Postgres.ConnectionString = "Host=localhost;Database=darling;Username=x;Password=y";
        config.Servers.Add(server);
        return config;
    }

    [Fact]
    public void TheFieldsReadFromDarlingJson_UnderTheirCamelCaseNames()
    {
        var json = """
            { "servers": [ { "name": "aurora-writer", "host": "h.example", "engine": "postgres",
                             "awsRoleArn": "arn:aws:iam::123456789012:role/darling-monitor", "awsExternalId": "tenant-1" } ] }
            """;

        var config = JsonSerializer.Deserialize<DarlingConfig>(json)!;
        var server = config.Servers.Single();

        Assert.Equal(Role, server.AwsRoleArn);
        Assert.Equal("tenant-1", server.AwsExternalId);
    }

    [Fact]
    public void AServerWithNoRole_HasNoKey_AndEveryExistingFileIsUnchanged()
    {
        Assert.Null(new MonitoredServer().AwsRoleArn);
        Assert.Null(new MonitoredServer().AwsExternalId);
        Assert.Null(new MonitoredServer().AwsRoleKey);
        Assert.Null(PgServer("   ", "tenant-1").AwsRoleKey);
    }

    [Fact]
    public void TheKeyIsTheTrimmedRoleAndExternalId_WithBlankAsNull()
    {
        Assert.Equal(new AwsRoleKey(Role, "tenant-1"), PgServer($"  {Role} ", " tenant-1 ").AwsRoleKey);
        Assert.Equal(new AwsRoleKey(Role, null), PgServer(Role, "  ").AwsRoleKey);
        Assert.Equal(new AwsRoleKey(Role, null), PgServer(Role, null).AwsRoleKey);

        /* Ordinal: an ARN and an external ID are case-sensitive, so a differently cased one is another key. */
        Assert.NotEqual(PgServer(Role, "Tenant-1").AwsRoleKey, PgServer(Role, "tenant-1").AwsRoleKey);
        Assert.NotEqual(PgServer(Role, null).AwsRoleKey, PgServer(Role.ToUpperInvariant(), null).AwsRoleKey);
    }

    [Fact]
    public void TheKeyIsNotSerialized()
    {
        var json = JsonSerializer.Serialize(PgServer(Role, "tenant-1"));

        Assert.Contains("awsRoleArn", json, StringComparison.Ordinal);
        Assert.DoesNotContain("AwsRoleKey", json, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Validate_AcceptsARoleWithAndWithoutAnExternalId_OnAPostgresTarget()
    {
        Assert.Empty(ConfigWith(PgServer(Role, null)).Validate());
        Assert.Empty(ConfigWith(PgServer(Role, "tenant-1")).Validate());
        Assert.Empty(ConfigWith(PgServer("  " + Role + "  ", "  tenant-1  ")).Validate());
        Assert.Empty(ConfigWith(PgServer(null, null)).Validate());
        Assert.Empty(ConfigWith(PgServer("", "")).Validate());
    }

    [Fact]
    public void Validate_RefusesAMalformedRole_WithTheServerLabelAndTheSharedSentence()
    {
        var problems = ConfigWith(PgServer("not-an-arn", null)).Validate();

        Assert.Equal($"server 'aurora-writer': {AwsRoleSettings.InvalidRoleMessage}", Assert.Single(problems));
    }

    [Fact]
    public void Validate_RefusesAMalformedExternalId()
    {
        var problems = ConfigWith(PgServer(Role, "has space")).Validate();

        Assert.Equal($"server 'aurora-writer': {AwsRoleSettings.InvalidExternalIdMessage}", Assert.Single(problems));
    }

    [Fact]
    public void Validate_RefusesAnExternalIdWithNoRole()
    {
        var problems = ConfigWith(PgServer(null, "tenant-1")).Validate();

        Assert.Equal($"server 'aurora-writer': {AwsRoleSettings.ExternalIdNeedsRoleMessage}", Assert.Single(problems));
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("")]
    public void Validate_RefusesARoleOnATargetThatIsNotPostgres(string engine)
    {
        var server = PgServer(Role, null);
        server.Engine = engine;
        server.Auth = "integrated";

        var problems = ConfigWith(server).Validate();

        Assert.Contains($"server 'aurora-writer': {AwsRoleSettings.RoleNeedsPostgresMessage}", problems);
    }

    [Fact]
    public void Validate_NeverEchoesTheExternalId()
    {
        var problems = ConfigWith(PgServer("not-an-arn", "tenant-secret-9")).Validate();

        Assert.All(problems, p => Assert.DoesNotContain("tenant-secret-9", p, StringComparison.Ordinal));
    }
}
