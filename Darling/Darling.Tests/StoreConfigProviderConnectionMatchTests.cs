/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A darling.json entry and a store row are the same connection only when they agree on all ten connection settings plus
/// engine (<c>ConnectionSettingsDiffer</c>, authentication compared ignoring case). The file entry's secret is copied into a
/// row that has none, and a reference the file declares is marked on a row, only for such a row.
/// </summary>
public sealed class StoreConfigProviderConnectionMatchTests
{
    private const string FileSecret = "file-secret-value-Q7";
    private const string DeclaredReference = "env:DARLING_TEST_DECLARED_REFERENCE_5374";

    private static MonitoredServer Entry() => new()
    {
        Name = "alpha",
        Host = "alpha.example.test",
        Port = 1433,
        Database = "master",
        Auth = "sql",
        Username = "monitor",
        EncryptMode = "Mandatory",
        TrustServerCertificate = false,
        MultiSubnetFailover = false,
        ReadOnlyIntent = false,
        Engine = "sqlserver",
        EncryptedPassword = FileSecret,
        EncryptedPasswordDeclaredByFile = true,
    };

    private static MonitoredServer RowLike(MonitoredServer entry, string? secret) => new()
    {
        Name = entry.Name,
        StoredServerId = entry.ServerId,
        Host = entry.Host,
        Port = entry.Port,
        Database = entry.Database,
        Auth = entry.Auth,
        Username = entry.Username,
        EncryptMode = entry.EncryptMode,
        TrustServerCertificate = entry.TrustServerCertificate,
        MultiSubnetFailover = entry.MultiSubnetFailover,
        ReadOnlyIntent = entry.ReadOnlyIntent,
        Engine = entry.Engine,
        EncryptedPassword = secret,
    };

    public static IEnumerable<object[]> OneFieldDiffers() =>
        new[] { "encrypt mode", "trust certificate", "multi-subnet", "auth kind", "username", "database", "read-only", "engine", "port", "host" }
            .Select(f => new object[] { f });

    private static void Change(string field, MonitoredServer r)
    {
        switch (field)
        {
            case "encrypt mode": r.EncryptMode = "Optional"; break;
            case "trust certificate": r.TrustServerCertificate = true; break;
            case "multi-subnet": r.MultiSubnetFailover = true; break;
            case "auth kind": r.Auth = "serviceprincipal"; break;
            case "username": r.Username = "other"; break;
            case "database": r.Database = "tempdb"; break;
            case "read-only": r.ReadOnlyIntent = true; break;
            case "engine": r.Engine = "postgresql"; break;
            case "port": r.Port = 1434; break;
            case "host": r.Host = "beta.example.test"; break;
            default: throw new ArgumentException(field);
        }
    }

    [Fact]
    public void TheEqualRow_GetsTheFileSecret()
    {
        var entry = Entry();
        var row = RowLike(entry, null);

        StoreConfigProvider.BackfillSecretFromFile(row, new DarlingConfig { Servers = [entry] });

        Assert.Equal(FileSecret, row.EncryptedPassword);
    }

    [Theory]
    [MemberData(nameof(OneFieldDiffers))]
    public void ARowThatDiffersInOneField_GetsNoFileSecret(string field)
    {
        var entry = Entry();
        var row = RowLike(entry, null);
        Change(field, row);

        StoreConfigProvider.BackfillSecretFromFile(row, new DarlingConfig { Servers = [entry] });

        Assert.True(string.IsNullOrEmpty(row.EncryptedPassword), field);
        Assert.Null(row.Password);
    }

    [Fact]
    public void TheEqualRow_GetsTheDeclaredMark()
    {
        var entry = Entry();
        entry.EncryptedPassword = DeclaredReference;
        var row = RowLike(entry, DeclaredReference);

        StoreConfigProvider.MarkSlotsTheFileDeclares(row, new DarlingConfig { Servers = [entry] });

        Assert.True(row.EncryptedPasswordDeclaredByFile);
    }

    [Theory]
    [MemberData(nameof(OneFieldDiffers))]
    public void ARowThatDiffersInOneField_GetsNoDeclaredMark(string field)
    {
        var entry = Entry();
        entry.EncryptedPassword = DeclaredReference;
        var row = RowLike(entry, DeclaredReference);
        Change(field, row);

        StoreConfigProvider.MarkSlotsTheFileDeclares(row, new DarlingConfig { Servers = [entry] });

        Assert.False(row.EncryptedPasswordDeclaredByFile, field);
    }
}
