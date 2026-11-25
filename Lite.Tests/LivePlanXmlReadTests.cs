/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9195: the live plan fetch reads its one value in a way whose cost follows the plan's size. A plan handle names a
/// whole batch, so the value can be ten megabytes, and the provider's asynchronous value reads cost the square of
/// that. These tests hold the read to its shape by counting the calls a fake command and reader receive: one
/// execution with <see cref="CommandBehavior.SequentialAccess"/>, one row read, one synchronous value read, and no
/// scalar or asynchronous value read. They also hold what the caller gets back: the same text, null for no plan,
/// and a cancellation, never a failed fetch, when the token fires during the read.
/// </summary>
public sealed class LivePlanXmlReadTests
{
    /// <summary>A batch plan of the shape that was slow in the field: 136 statements and over ten million characters.</summary>
    private static string BatchPlan()
    {
        var operators = new StringBuilder();
        for (var node = 0; node < 38; node++)
        {
            operators.Append("<RelOp NodeId=\"").Append(node).Append("\" PhysicalOp=\"Clustered Index Seek\" LogicalOp=\"Clustered Index Seek\" EstimateRows=\"1\">")
                .Append("<OutputList>").Append(new string('c', 1900)).Append("</OutputList></RelOp>");
        }

        var plan = new StringBuilder("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\"><BatchSequence><Batch><Statements>");
        for (var statement = 1; statement <= 136; statement++)
        {
            plan.Append("<StmtSimple StatementId=\"").Append(statement).Append("\" StatementType=\"SELECT\"><QueryPlan>")
                .Append(operators).Append("</QueryPlan></StmtSimple>");
        }

        return plan.Append("</Statements></Batch></BatchSequence></ShowPlanXML>").ToString();
    }

    [Fact]
    public async Task ABatchPlanOfTenMillionCharacters_ComesBackWhole_FromOneSequentialRowAndOneValueRead()
    {
        var plan = BatchPlan();
        Assert.True(plan.Length > 10_000_000, $"the generated plan is {plan.Length:N0} characters");
        var command = new FakePlanCommand(new FakePlanReader(plan));

        var read = await LivePlanXmlRead.FirstValueAsync(command, CancellationToken.None);

        Assert.Equal(plan.Length, read!.Length);
        Assert.True(string.Equals(plan, read, StringComparison.Ordinal), "the plan text came back changed");
        Assert.Equal(new[] { CommandBehavior.SequentialAccess }, command.ReaderBehaviors);
        Assert.Equal(0, command.ScalarCalls);
        Assert.Equal(1, command.Reader.RowReads);
        Assert.Equal(1, command.Reader.ValueReads);
        Assert.Equal(0, command.Reader.AsyncValueReads);
        Assert.True(command.Reader.IsClosed);
        Assert.Equal(0, command.CancelCalls);
    }

    [Fact]
    public async Task APlanNoLongerInCache_IsNull_WhetherTheRowIsMissingOrItsValueIsNull()
    {
        var noRow = new FakePlanCommand(new FakePlanReader());
        Assert.Null(await LivePlanXmlRead.FirstValueAsync(noRow, CancellationToken.None));
        Assert.Equal(1, noRow.Reader.RowReads);
        Assert.Equal(0, noRow.Reader.ValueReads);

        var nullValue = new FakePlanCommand(new FakePlanReader(DBNull.Value));
        Assert.Null(await LivePlanXmlRead.FirstValueAsync(nullValue, CancellationToken.None));
        Assert.Equal(1, nullValue.Reader.ValueReads);
    }

    [Fact]
    public async Task TheCallersToken_ReachesTheExecutionAndTheRowRead()
    {
        using var source = new CancellationTokenSource();
        var command = new FakePlanCommand(new FakePlanReader("<ShowPlanXML />"));

        await LivePlanXmlRead.FirstValueAsync(command, source.Token);

        Assert.Equal(source.Token, command.ExecuteToken);
        Assert.Equal(source.Token, command.Reader.RowReadToken);
    }

    [Fact]
    public async Task ATokenThatFiresDuringTheValueRead_CancelsTheCommand_AndIsReportedAsCancellation()
    {
        using var source = new CancellationTokenSource();
        var providerFailure = new InvalidOperationException("Operation cancelled by user.");
        var reader = new FakePlanReader("<ShowPlanXML />");
        var command = new FakePlanCommand(reader);
        // The token fires while the value read is in progress, and the provider then fails the read it was told to stop.
        reader.DuringValueRead = () =>
        {
            source.Cancel();
            throw providerFailure;
        };

        var cancelled = await Assert.ThrowsAsync<OperationCanceledException>(
            () => LivePlanXmlRead.FirstValueAsync(command, source.Token));

        Assert.Equal(source.Token, cancelled.CancellationToken);
        Assert.Same(providerFailure, cancelled.InnerException);
        Assert.Equal(1, command.CancelCalls);
        Assert.True(reader.IsClosed);
    }

    [Fact]
    public async Task AReadThatFailsUnderALiveToken_KeepsItsOwnException()
    {
        using var source = new CancellationTokenSource();
        var providerFailure = new InvalidOperationException("Execution Timeout Expired.");
        var reader = new FakePlanReader("<ShowPlanXML />") { DuringValueRead = () => throw providerFailure };
        var command = new FakePlanCommand(reader);

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
            () => LivePlanXmlRead.FirstValueAsync(command, source.Token));

        Assert.Same(providerFailure, thrown);
        Assert.Equal(0, command.CancelCalls);
    }

    [Fact]
    public async Task ATokenThatFiresAfterTheRead_NoLongerCancelsTheCommand()
    {
        using var source = new CancellationTokenSource();
        var command = new FakePlanCommand(new FakePlanReader("<ShowPlanXML />"));

        Assert.Equal("<ShowPlanXML />", await LivePlanXmlRead.FirstValueAsync(command, source.Token));
        source.Cancel();

        Assert.Equal(0, command.CancelCalls);
    }

    /// <summary>A command that records how it was executed and hands back the reader it was built with.</summary>
    private sealed class FakePlanCommand : DbCommand
    {
        public FakePlanCommand(FakePlanReader reader) => Reader = reader;

        public FakePlanReader Reader { get; }
        public List<CommandBehavior> ReaderBehaviors { get; } = new();
        public CancellationToken ExecuteToken { get; private set; }
        public int ScalarCalls { get; private set; }
        public int CancelCalls { get; private set; }

        [AllowNull]
        public override string CommandText { get; set; } = string.Empty;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get; set; }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override void Cancel() => CancelCalls++;
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();

        public override object? ExecuteScalar()
        {
            ScalarCalls++;
            return Reader.FirstValueForScalar;
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            ReaderBehaviors.Add(behavior);
            return Reader;
        }

        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken)
        {
            ExecuteToken = cancellationToken;
            return Task.FromResult(ExecuteDbDataReader(behavior));
        }
    }

    /// <summary>A reader of at most one row with one column, which counts each way its value can be read.</summary>
    private sealed class FakePlanReader : DbDataReader
    {
        private readonly object? _value;
        private readonly bool _hasRow;
        private bool _closed;

        public FakePlanReader() { }

        public FakePlanReader(object value)
        {
            _value = value;
            _hasRow = true;
        }

        public Action? DuringValueRead { get; set; }
        public int RowReads { get; private set; }
        public int ValueReads { get; private set; }
        public int AsyncValueReads { get; private set; }
        public CancellationToken RowReadToken { get; private set; }
        public object? FirstValueForScalar => _hasRow ? _value : null;

        public override bool Read()
        {
            RowReads++;
            return _hasRow && RowReads == 1;
        }

        public override Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            RowReadToken = cancellationToken;
            return Task.FromResult(Read());
        }

        public override object GetValue(int ordinal)
        {
            Assert.Equal(0, ordinal);
            ValueReads++;
            DuringValueRead?.Invoke();
            return _value!;
        }

        public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken)
        {
            AsyncValueReads++;
            return Task.FromResult((T)_value!);
        }

        public override void Close() => _closed = true;
        public override bool IsClosed => _closed;
        public override bool NextResult() => false;
        public override int FieldCount => 1;
        public override bool HasRows => _hasRow;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int ordinal] => throw new NotSupportedException();
        public override object this[string name] => throw new NotSupportedException();
        public override bool GetBoolean(int ordinal) => throw new NotSupportedException();
        public override byte GetByte(int ordinal) => throw new NotSupportedException();
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override char GetChar(int ordinal) => throw new NotSupportedException();
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
        public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();
        public override DateTime GetDateTime(int ordinal) => throw new NotSupportedException();
        public override decimal GetDecimal(int ordinal) => throw new NotSupportedException();
        public override double GetDouble(int ordinal) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int ordinal) => throw new NotSupportedException();
        public override float GetFloat(int ordinal) => throw new NotSupportedException();
        public override Guid GetGuid(int ordinal) => throw new NotSupportedException();
        public override short GetInt16(int ordinal) => throw new NotSupportedException();
        public override int GetInt32(int ordinal) => throw new NotSupportedException();
        public override long GetInt64(int ordinal) => throw new NotSupportedException();
        public override string GetName(int ordinal) => throw new NotSupportedException();
        public override int GetOrdinal(string name) => throw new NotSupportedException();
        public override string GetString(int ordinal) => throw new NotSupportedException();
        public override int GetValues(object[] values) => throw new NotSupportedException();
        public override bool IsDBNull(int ordinal) => throw new NotSupportedException();
    }
}
