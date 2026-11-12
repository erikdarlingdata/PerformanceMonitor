/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information
 */

using System;
using System.Collections.Generic;
using System.IO;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8239: the compose quick start names its server with the key darling.json reads, and a key in a
/// <c>servers</c> entry that no setting declares is named at start, the way an unknown <c>web</c> or
/// <c>mcp</c> key is. The <c>// </c>-prefixed comment keys the sample files carry stay allowed.
/// </summary>
public sealed class ComposeSampleServerNameTests
{
    /// <summary>The shipped compose sample, with each docker secret reference swapped for an inline value so the
    /// parse does not read /run/secrets. The keys are the sample's own.</summary>
    private static string ComposeSampleJson() =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "compose.darling.sample.json"))
            .Replace("file:/run/secrets/store_connection", "Host=store;Database=darling", StringComparison.Ordinal)
            .Replace("file:/run/secrets/sql_password", "placeholder-not-a-secret", StringComparison.Ordinal)
            .Replace("file:/run/secrets/web_token", "placeholder-not-a-secret", StringComparison.Ordinal)
            .Replace("file:/run/secrets/mcp_token", "placeholder-not-a-secret", StringComparison.Ordinal);

    private static IReadOnlyList<string> WarningsFor(string json) =>
        DarlingWorker.GetUnknownNetworkKeyWarnings(DarlingConfig.Parse(json));

    [Fact]
    public void TheComposeSample_NamesItsFirstServer()
    {
        var config = DarlingConfig.Parse(ComposeSampleJson());

        var server = Assert.Single(config.Servers);
        Assert.Equal("prod-sql-01", server.Name);
        Assert.Equal("prod-sql-01", server.DisplayName);
    }

    [Fact]
    public void TheComposeSampleAsShipped_GivesNoUnknownKeyWarning()
    {
        var warnings = WarningsFor(ComposeSampleJson());

        Assert.Empty(warnings);
    }

    [Theory]
    [InlineData("""{ "servers": [ { "host": "sql01", "displayName": "prod-sql-01" } ] }""", "servers[0].displayName")]
    [InlineData("""{ "servers": [ { "host": "sql01" }, { "host": "sql02", "excludedDatabase": [ "x" ] } ] }""", "servers[1].excludedDatabase")]
    public void AServersEntryWithAnUnknownKey_GetsOneWarningNamingItsPath(string json, string path)
    {
        var warning = Assert.Single(WarningsFor(json));

        Assert.Contains($"'{path}'", warning, StringComparison.Ordinal);
        Assert.DoesNotContain("prod-sql-01", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void AServersEntryWithCommentKeysAndEveryDeclaredKey_GivesNoWarning()
    {
        /* The declared keys in a different case on purpose, as the loader binds them, beside two "// "-prefixed notes
           of the kind the sample files carry. */
        var warnings = WarningsFor("""
            { "servers": [ {
                "// host": "the host, with a port after a comma",
                "Name": "s1", "ENGINE": "sqlserver", "host": "sql01", "port": 1433, "database": null, "auth": "sql",
                "username": "u", "encryptedPassword": null, "password": "placeholder-not-a-secret", "readOnlyIntent": false,
                "trustServerCertificate": true, "encryptMode": "Mandatory", "multiSubnetFailover": false,
                "queryStoreReplica": null, "excludedDatabases": [], "xeSessionDatabases": [], "monthlyCostUsd": 0,
                "alertDeliveryModeOverride": null, "remediationUsername": null, "remediationEncryptedPassword": null,
                "hypotheticalIndexUsername": null, "hypotheticalIndexEncryptedPassword": null,
                "awsRoleArn": null, "awsExternalId": null,
                "// trustServerCertificate": "true accepts any certificate"
            } ] }
            """);

        Assert.Empty(warnings);
    }
}
