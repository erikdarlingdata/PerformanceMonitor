using System;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace Darling.Tests;

/// <summary>Builds a command for a stderr-tail consumer's query with its resume parameters bound the way the target provider binds them.</summary>
internal static class LiveTailQuery
{
    public static NpgsqlCommand Command(CollectorQuery query, NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand(query.Text, connection);
        foreach (var parameter in query.Parameters)
        {
            command.Parameters.Add(new NpgsqlParameter(
                parameter.Name,
                parameter.Type == CollectorParameterType.BigInt ? NpgsqlDbType.Bigint : NpgsqlDbType.Text)
            { Value = parameter.Value ?? DBNull.Value });
        }

        return command;
    }
}
