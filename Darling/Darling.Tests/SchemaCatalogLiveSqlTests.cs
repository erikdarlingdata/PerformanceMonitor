/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643: the catalog read batches against a real SQL Server. Gated on <c>DARLING_TEST_SQL</c> (optional
/// <c>DARLING_TEST_SQL_USER</c> / <c>DARLING_TEST_SQL_PASSWORD</c>), so it skips where the runner has no SQL Server. Each test creates
/// and drops its own scratch database; <c>DARLING_TEST_SQL_READONLY=1</c> skips the class.
/// </summary>
public sealed class SchemaCatalogLiveSqlTests
{
    private static string? ConnectionString(string applicationName, string database = "master")
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host) || Environment.GetEnvironmentVariable("DARLING_TEST_SQL_READONLY") == "1")
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = applicationName,
            Pooling = false,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live schema catalog tests; DARLING_TEST_SQL_READONLY=1 skips them.";

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string ScratchName() => "SchemaDb_" + Guid.NewGuid().ToString("N")[..10];

    private static async Task<SchemaCatalogSnapshot> ReadAsync(string connectionString, string batch, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(batch, connection) { CommandTimeout = 60 };
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await SchemaCatalogReader.ReadAsync(reader, ct);
    }

    private static async Task WithScratchDatabaseAsync(string? createOptions, Func<string, CancellationToken, Task> body)
    {
        const string app = "pm-7643-catalog-live-test";
        var master = ConnectionString(app);
        Assert.SkipWhen(master is null, SkipMessage);
        var ct = TestContext.Current.CancellationToken;
        var name = ScratchName();

        if (createOptions != null)
        {
            await ExecuteAsync(master!, "EXEC sp_configure 'contained database authentication', 1; RECONFIGURE;", ct);
        }

        await ExecuteAsync(master!, "CREATE DATABASE [" + name + "] " + createOptions, ct);
        try
        {
            await body(ConnectionString(app, name)!, ct);
        }
        finally
        {
            await ExecuteAsync(master!, "ALTER DATABASE [" + name + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [" + name + "];", CancellationToken.None);
        }
    }

    private static async Task CreateRichSchemaAsync(string db, CancellationToken ct)
    {
        string[] statements =
        [
            "CREATE TABLE dbo.Customers (CustomerId int NOT NULL CONSTRAINT PK_Customers PRIMARY KEY)",
            @"CREATE TABLE dbo.Orders (
                OrderId int IDENTITY(5, 2) NOT NULL,
                CustomerCode nvarchar(20) COLLATE Latin1_General_100_CS_AS NOT NULL,
                Amount decimal(12, 2) NOT NULL CONSTRAINT DF_Orders_Amount DEFAULT (0),
                Qty int NOT NULL,
                Total AS (Amount * Qty) PERSISTED,
                Note varchar(50) SPARSE NULL,
                RowGuid uniqueidentifier ROWGUIDCOL NOT NULL DEFAULT NEWID(),
                CustomerId int NULL CONSTRAINT FK_Orders_Customers FOREIGN KEY REFERENCES dbo.Customers (CustomerId) ON DELETE CASCADE,
                CONSTRAINT PK_Orders PRIMARY KEY CLUSTERED (OrderId),
                CONSTRAINT UQ_Orders_Code UNIQUE (CustomerCode),
                CONSTRAINT CK_Orders_Qty CHECK (Qty > 0))",
            "CREATE INDEX IX_Orders_Amount ON dbo.Orders (Amount DESC, Qty) INCLUDE (CustomerCode, Total) WHERE Amount > 0",
            "CREATE TRIGGER dbo.TR_Orders_After ON dbo.Orders AFTER INSERT, UPDATE AS SET NOCOUNT ON;",
            "CREATE TRIGGER dbo.TR_Orders_Instead ON dbo.Orders INSTEAD OF INSERT AS SET NOCOUNT ON;",
            "CREATE VIEW dbo.OrderTotals WITH SCHEMABINDING AS SELECT OrderId, Amount FROM dbo.Orders",
            "CREATE VIEW dbo.OrderCounts WITH SCHEMABINDING AS SELECT CustomerId, COUNT_BIG(*) AS Cnt FROM dbo.Orders GROUP BY CustomerId",
            "CREATE UNIQUE CLUSTERED INDEX IX_OrderCounts ON dbo.OrderCounts (CustomerId)",
            "CREATE TABLE dbo.Heap1 (A int NOT NULL)",
        ];

        foreach (var statement in statements)
        {
            await ExecuteAsync(db, statement.StartsWith("CREATE UNIQUE CLUSTERED", StringComparison.Ordinal) ? "SET ARITHABORT ON; " + statement : statement, ct);
        }
    }

    private static int ScaleOf(decimal value) => (decimal.GetBits(value)[3] >> 16) & 0xFF;

    [Fact]
    public async Task CatalogRead_ReadsColumnsIndexesConstraintsTriggersAndViewText()
    {
        await WithScratchDatabaseAsync(null, async (db, ct) =>
        {
            await CreateRichSchemaAsync(db, ct);

            var snap = await ReadAsync(db, SchemaCatalogQueries.BuildFullRead(), ct);

            Assert.True(snap.HasViewDefinition);
            Assert.False(snap.OverCap);
            Assert.Equal(new[] { "Customers", "Orders", "OrderTotals", "OrderCounts", "Heap1" }.Order(), snap.Objects.Select(o => o.Name).Order());
            Assert.All(snap.Objects, o => Assert.Equal("dbo", o.Schema));

            var orders = snap.Objects.Single(o => o.Name == "Orders");
            Assert.Equal('U', orders.Type);
            Assert.NotNull(orders.Table);
            Assert.Null(orders.View);
            Assert.Equal("NON_TEMPORAL_TABLE", orders.Table!.TemporalType);
            Assert.Null(orders.Table.HistoryTable);
            Assert.Equal(false, orders.Table.IsMemoryOptimized);
            Assert.Equal("SCHEMA_AND_DATA", orders.Table.Durability);
            Assert.Null(orders.Table.PartitionScheme);
            Assert.Equal("TABLE", orders.Table.LockEscalation);
            Assert.NotNull(orders.ModifyDate);

            Assert.Equal(["OrderId", "CustomerCode", "Amount", "Qty", "Total", "Note", "RowGuid", "CustomerId"], orders.Columns.Select(c => c.Name));
            Assert.Equal([1, 2, 3, 4, 5, 6, 7, 8], orders.Columns.Select(c => c.Ordinal));

            var id = orders.Columns[0];
            Assert.Equal("int", id.TypeName);
            Assert.Equal(5m, id.IdentitySeed);
            Assert.Equal(2m, id.IdentityIncrement);
            Assert.Equal(0, ScaleOf(id.IdentitySeed!.Value));
            Assert.Equal(0, ScaleOf(id.IdentityIncrement!.Value));
            Assert.False(id.IsNullable);
            Assert.Null(id.ComputedDefinition);

            var code = orders.Columns[1];
            Assert.Equal("nvarchar", code.TypeName);
            Assert.Equal(40, code.MaxLength);
            Assert.Equal("Latin1_General_100_CS_AS", code.CollationName);
            Assert.Null(code.IdentitySeed);

            var amount = orders.Columns[2];
            Assert.Equal("decimal", amount.TypeName);
            Assert.Equal(12, amount.Precision);
            Assert.Equal(2, amount.Scale);
            Assert.Equal("DF_Orders_Amount", amount.DefaultName);
            Assert.Equal("((0))", amount.DefaultDefinition);

            var total = orders.Columns[4];
            Assert.Equal("([Amount]*[Qty])", total.ComputedDefinition);
            Assert.Equal(true, total.IsPersisted);

            Assert.Equal(true, orders.Columns[5].IsSparse);
            Assert.True(orders.Columns[5].IsNullable);
            Assert.Equal(true, orders.Columns[6].IsRowGuidCol);
            Assert.NotNull(orders.Columns[6].DefaultName);

            Assert.Equal(["IX_Orders_Amount", "PK_Orders", "UQ_Orders_Code"], orders.Indexes.Select(i => i.Name!).Order());
            var pk = orders.Indexes.Single(i => i.Name == "PK_Orders");
            Assert.Equal("CLUSTERED", pk.TypeDesc);
            Assert.True(pk.IsPrimaryKey);
            Assert.True(pk.IsUnique);
            Assert.False(pk.IsUniqueConstraint);
            Assert.Equal(["OrderId"], pk.KeyColumns);
            Assert.Empty(pk.IncludedColumns);
            Assert.Equal(1, pk.IndexId);

            var uq = orders.Indexes.Single(i => i.Name == "UQ_Orders_Code");
            Assert.True(uq.IsUniqueConstraint);
            Assert.Equal("NONCLUSTERED", uq.TypeDesc);

            var filtered = orders.Indexes.Single(i => i.Name == "IX_Orders_Amount");
            Assert.Equal(["Amount DESC", "Qty"], filtered.KeyColumns);
            Assert.Equal(["CustomerCode", "Total"], filtered.IncludedColumns.Order());
            Assert.Equal("([Amount]>(0))", filtered.FilterDefinition);
            Assert.Equal("NONE", filtered.DataCompression);
            Assert.False(filtered.IsDisabled);
            Assert.False(filtered.IsUnique);

            Assert.Equal(["C:CK_Orders_Qty", "D:DF_Orders_Amount", "F:FK_Orders_Customers", "P:PK_Orders", "U:UQ_Orders_Code"],
                orders.Constraints.Where(c => !c.IsSystemNamed).Select(c => c.Kind + ":" + c.Name).Order());
            var ck = orders.Constraints.Single(c => c.Kind == 'C');
            Assert.Equal("([Qty]>(0))", ck.Definition);
            Assert.False(ck.IsDisabled);
            Assert.False(ck.IsNotTrusted);
            Assert.False(ck.IsSystemNamed);
            var fk = orders.Constraints.Single(c => c.Kind == 'F');
            Assert.Equal(["CustomerId"], fk.FkColumns);
            Assert.Equal("dbo", fk.ReferencedSchema);
            Assert.Equal("Customers", fk.ReferencedTable);
            Assert.Equal(["CustomerId"], fk.ReferencedColumns);
            Assert.Equal("CASCADE", fk.DeleteAction);
            Assert.Equal("NO_ACTION", fk.UpdateAction);
            var systemNamedDefault = orders.Constraints.Single(c => c.Kind == 'D' && c.IsSystemNamed);
            Assert.Equal("(newid())", systemNamedDefault.Definition);
            Assert.Equal(orders.Columns[6].DefaultName, systemNamedDefault.Name);

            Assert.Equal(["TR_Orders_After", "TR_Orders_Instead"], orders.Triggers.Select(t => t.Name).Order());
            var after = orders.Triggers.Single(t => t.Name == "TR_Orders_After");
            Assert.False(after.IsInsteadOf);
            Assert.Equal("INSERT,UPDATE", after.Events);
            var instead = orders.Triggers.Single(t => t.Name == "TR_Orders_Instead");
            Assert.True(instead.IsInsteadOf);
            Assert.Equal("INSERT", instead.Events);

            var view = snap.Objects.Single(o => o.Name == "OrderTotals");
            Assert.Equal('V', view.Type);
            Assert.Null(view.Table);
            Assert.NotNull(view.View);
            Assert.Equal(true, view.View!.IsSchemaBound);
            Assert.Equal(false, view.View.IsIndexed);
            Assert.Contains("SELECT OrderId, Amount FROM dbo.Orders", view.View.ViewDefinition, StringComparison.Ordinal);
            Assert.Equal(["OrderId", "Amount"], view.Columns.Select(c => c.Name));

            var indexedView = snap.Objects.Single(o => o.Name == "OrderCounts");
            Assert.Equal(true, indexedView.View!.IsIndexed);
            Assert.Equal(["IX_OrderCounts"], indexedView.Indexes.Select(i => i.Name));

            var heap = snap.Objects.Single(o => o.Name == "Heap1");
            var heapIndex = Assert.Single(heap.Indexes);
            Assert.Null(heapIndex.Name);
            Assert.Equal("HEAP", heapIndex.TypeDesc);
            Assert.Equal(0, heapIndex.IndexId);
            Assert.Empty(heapIndex.KeyColumns);
        });
    }

    [Fact]
    public async Task CatalogRead_TwoReadsOfTheSameSchema_HashTheSame()
    {
        await WithScratchDatabaseAsync(null, async (db, ct) =>
        {
            await CreateRichSchemaAsync(db, ct);

            var first = await ReadAsync(db, SchemaCatalogQueries.BuildFullRead(), ct);
            var second = await ReadAsync(db, SchemaCatalogQueries.BuildFullRead(), ct);

            Assert.Equal(first.Objects.Count, second.Objects.Count);
            foreach (var a in first.Objects)
            {
                var b = second.Objects.Single(o => o.ObjectId == a.ObjectId);
                Assert.Equal(SchemaCanonicalForm.Hash(a), SchemaCanonicalForm.Hash(b));
            }

            await ExecuteAsync(db, "ALTER TABLE dbo.Orders ADD Extra int NULL", ct);
            var third = await ReadAsync(db, SchemaCatalogQueries.BuildFullRead(), ct);
            var before = first.Objects.Single(o => o.Name == "Orders");
            var after = third.Objects.Single(o => o.Name == "Orders");
            Assert.NotEqual(SchemaCanonicalForm.Hash(before), SchemaCanonicalForm.Hash(after));
        });
    }

    [Fact]
    public async Task CatalogRead_Targeted_ReturnsOnlyThoseObjects()
    {
        await WithScratchDatabaseAsync(null, async (db, ct) =>
        {
            await CreateRichSchemaAsync(db, ct);
            var full = await ReadAsync(db, SchemaCatalogQueries.BuildFullRead(), ct);
            var orders = full.Objects.Single(o => o.Name == "Orders");
            var heap = full.Objects.Single(o => o.Name == "Heap1");

            var targeted = await ReadAsync(db, SchemaCatalogQueries.BuildTargetedRead([orders.ObjectId, heap.ObjectId, -12345]), ct);

            Assert.True(targeted.HasViewDefinition);
            Assert.False(targeted.OverCap);
            Assert.Equal(["Heap1", "Orders"], targeted.Objects.Select(o => o.Name).Order());
            Assert.Equal(SchemaCanonicalForm.Hash(orders), SchemaCanonicalForm.Hash(targeted.Objects.Single(o => o.Name == "Orders")));

            var none = await ReadAsync(db, SchemaCatalogQueries.BuildTargetedRead([]), ct);
            Assert.Empty(none.Objects);
        });
    }

    [Fact]
    public async Task CatalogRead_OverCap_IsFlagged()
    {
        await WithScratchDatabaseAsync(null, async (db, ct) =>
        {
            foreach (var n in new[] { "T1", "T2", "T3" })
            {
                await ExecuteAsync(db, "CREATE TABLE dbo." + n + " (A int NOT NULL)", ct);
            }

            var capped = await ReadAsync(db, SchemaCatalogQueries.BuildFullRead(2), ct);
            Assert.True(capped.OverCap);
            Assert.Equal(["T1", "T2"], capped.Objects.Select(o => o.Name));

            var exact = await ReadAsync(db, SchemaCatalogQueries.BuildFullRead(3), ct);
            Assert.False(exact.OverCap);
            Assert.Equal(3, exact.Objects.Count);
        });
    }

    [Fact]
    public async Task CatalogRead_WithoutViewDefinition_ReportsIncomplete()
    {
        await WithScratchDatabaseAsync("CONTAINMENT = PARTIAL", async (db, ct) =>
        {
            var password = "Pw!" + Guid.NewGuid().ToString("N") + "aA1";
            await ExecuteAsync(db, "CREATE TABLE dbo.Orders (A int NOT NULL)", ct);
            await ExecuteAsync(db, "CREATE USER [reader_user] WITH PASSWORD = N'" + password + "'", ct);
            await ExecuteAsync(db, "GRANT SELECT ON dbo.Orders TO [reader_user]", ct);

            var builder = new SqlConnectionStringBuilder(db) { IntegratedSecurity = false, UserID = "reader_user", Password = password };

            var owner = await ReadAsync(db, SchemaCatalogQueries.BuildFullRead(), ct);
            Assert.True(owner.HasViewDefinition);

            var restricted = await ReadAsync(builder.ConnectionString, SchemaCatalogQueries.BuildFullRead(), ct);
            Assert.False(restricted.HasViewDefinition);
        });
    }
}
