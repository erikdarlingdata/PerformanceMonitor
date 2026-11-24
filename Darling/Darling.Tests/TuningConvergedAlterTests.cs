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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8949: the tuning pass issues an <c>ALTER TABLE ... SET (...)</c> only when the catalog shows a setting missing or different.
/// These pin the two pure halves of that check: the parse of a statement into its table and options, and the comparison of
/// those options with a <c>pg_class.reloptions</c> array. <see cref="TuningConvergedAlterLiveTests"/> holds the catalog half.
/// </summary>
public sealed class TuningConvergedAlterTests
{
    private static readonly KeyValuePair<string, string>[] Wanted =
    {
        new("autovacuum_vacuum_scale_factor", "0.02"),
        new("autovacuum_vacuum_threshold", "10000"),
    };

    /// <summary>A statement that does not parse is issued on every pass, so every one in the list has to parse, whole.</summary>
    [Fact]
    public void EveryAlterTableStatement_ParsesIntoItsTableAndEveryOption()
    {
        var alters = PgTableTuning.Statements.Where(s => s.StartsWith("ALTER TABLE ", StringComparison.Ordinal)).ToList();
        Assert.NotEmpty(alters);

        foreach (var statement in alters)
        {
            var parsed = PgTableTuning.ParseAlterSet(statement);
            Assert.True(parsed is not null, "does not parse: " + statement);
            var (table, options) = parsed!.Value;
            Assert.StartsWith("collect.", table, StringComparison.Ordinal);
            Assert.NotEmpty(options);

            /* Rebuilt from the parse it is the statement again, so the parse dropped no option and no part of a value. */
            Assert.Equal(statement, "ALTER TABLE " + table + " SET (" + string.Join(", ", options.Select(o => o.Key + " = " + o.Value)) + ")");
        }
    }

    [Fact]
    public void AStatement_ParsesIntoItsTable_AndEachKeyWithItsValue()
    {
        var parsed = PgTableTuning.ParseAlterSet(
            "ALTER TABLE collect.oversized_plan_backlog SET (fillfactor = 80, autovacuum_vacuum_scale_factor = 0.02,autovacuum_vacuum_threshold=1000)");

        Assert.NotNull(parsed);
        Assert.Equal("collect.oversized_plan_backlog", parsed!.Value.Table);
        Assert.Equal(
            new KeyValuePair<string, string>[] { new("fillfactor", "80"), new("autovacuum_vacuum_scale_factor", "0.02"), new("autovacuum_vacuum_threshold", "1000") },
            parsed.Value.Options);
    }

    [Theory]
    [InlineData("CREATE INDEX IF NOT EXISTS idx_x ON collect.t (a)")]
    [InlineData("ALTER TABLE collect.t RESET (fillfactor)")]
    [InlineData("ALTER TABLE collect.t SET ()")]
    [InlineData("ALTER TABLE collect.t SET (fillfactor)")]
    [InlineData("ALTER TABLE collect.t SET (fillfactor = )")]
    [InlineData("ALTER TABLE collect.t SET (fillfactor = 80")]
    [InlineData("ALTER TABLE collect.t SET (fillfactor = 80);")]
    [InlineData("ALTER TABLE collect.t SET (fillfactor = '80')")]
    [InlineData("ALTER TABLE collect.t SET (toast.autovacuum_enabled = false)")]
    [InlineData("ALTER TABLE ONLY collect.t SET (fillfactor = 80)")]
    public void AStatementOfAnyOtherShape_DoesNotParse(string statement)
    {
        Assert.Null(PgTableTuning.ParseAlterSet(statement));
    }

    [Fact]
    public void TheSameValues_AreConverged()
    {
        Assert.True(PgTableTuning.ReloptionsCarry(Wanted, new[] { "autovacuum_vacuum_scale_factor=0.02", "autovacuum_vacuum_threshold=10000" }));
        Assert.True(PgTableTuning.ReloptionsCarry(Wanted, new[] { "autovacuum_vacuum_threshold=10000", "autovacuum_vacuum_scale_factor=0.02" }));
    }

    /// <summary>The key is there with another value: a check on the key alone would call this converged.</summary>
    [Fact]
    public void ADifferentValue_IsNotConverged()
    {
        Assert.False(PgTableTuning.ReloptionsCarry(Wanted, new[] { "autovacuum_vacuum_scale_factor=0.2", "autovacuum_vacuum_threshold=10000" }));
        Assert.False(PgTableTuning.ReloptionsCarry(Wanted, new[] { "autovacuum_vacuum_scale_factor=0.02", "autovacuum_vacuum_threshold=1000" }));
    }

    /// <summary>The catalog keeps the spelling an option was set with, so the same number written another way is the same setting.</summary>
    [Theory]
    [InlineData("0.020")]
    [InlineData(".02")]
    [InlineData("2e-2")]
    [InlineData("0.02")]
    public void TheSameNumber_SpelledAnotherWay_IsConverged(string spelling)
    {
        Assert.True(PgTableTuning.ReloptionsCarry(Wanted, new[] { "autovacuum_vacuum_scale_factor=" + spelling, "autovacuum_vacuum_threshold=10000.0" }));
    }

    [Fact]
    public void AMissingKey_IsNotConverged()
    {
        Assert.False(PgTableTuning.ReloptionsCarry(Wanted, new[] { "autovacuum_vacuum_scale_factor=0.02" }));
        Assert.False(PgTableTuning.ReloptionsCarry(Wanted, Array.Empty<string>()));
        Assert.False(PgTableTuning.ReloptionsCarry(Wanted, null));

        /* The insert knob is another key, not this one with a longer name. */
        Assert.False(PgTableTuning.ReloptionsCarry(Wanted, new[] { "autovacuum_vacuum_insert_scale_factor=0.02", "autovacuum_vacuum_insert_threshold=10000" }));
    }

    [Fact]
    public void AnOptionTheStatementDoesNotSet_IsIgnored()
    {
        Assert.True(PgTableTuning.ReloptionsCarry(
            Wanted,
            new[] { "fillfactor=90", "autovacuum_vacuum_scale_factor=0.02", "autovacuum_enabled=false", "autovacuum_vacuum_threshold=10000" }));
    }

    [Fact]
    public void KeysCompareWithoutCase_AndATextValueComparesWithoutCase()
    {
        var wanted = new KeyValuePair<string, string>[] { new("Autovacuum_Enabled", "off") };
        Assert.True(PgTableTuning.ReloptionsCarry(wanted, new[] { "autovacuum_enabled=OFF" }));

        /* Another spelling of the same boolean is a mismatch: the ALTER is issued and stores the statement's spelling. */
        Assert.False(PgTableTuning.ReloptionsCarry(wanted, new[] { "autovacuum_enabled=false" }));
    }

    /// <summary>A catalog read that throws answers "not converged" and does not throw, so the caller issues the ALTER.</summary>
    [Fact]
    public async Task ACatalogReadThatFails_AnswersNotConverged_AndDoesNotThrow()
    {
        await using var neverOpened = new NpgsqlConnection("Host=localhost;Database=never_opened");
        var statement = PgTableTuning.Statements.First(s => s.StartsWith("ALTER TABLE collect.query_plan_dim ", StringComparison.Ordinal));
        var log = new CapturingTestLogger();

        Assert.False(await PgTableTuning.AlterIsConvergedAsync(neverOpened, statement, log, TestContext.Current.CancellationToken));
        Assert.Contains(log.Lines, l => l.StartsWith("Debug:", StringComparison.Ordinal) && l.Contains("collect.query_plan_dim", StringComparison.Ordinal));
    }
}
