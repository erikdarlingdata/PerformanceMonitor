using System.Globalization;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Maps a PostgreSQL data type name (as Npgsql's GetDataTypeName reports it) to the DuckDB
/// column type the exporter writes. Anything not in the map fails loudly, naming the column,
/// rather than silently guessing.
/// </summary>
public static partial class DuckDbTypeMapper
{
    /// <summary>DuckDB's DECIMAL holds at most 38 digits; wider Postgres numerics fall back to DOUBLE.</summary>
    private const int MaxDuckDbDecimalPrecision = 38;

    [GeneratedRegex(@"^(?:numeric|decimal)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)$")]
    private static partial Regex ModifiedNumeric();

    /// <summary>
    /// Maps one column's Postgres type name to a DuckDB type. Throws
    /// <see cref="NotSupportedException"/> naming the column and type if there is no mapping.
    /// </summary>
    public static string Map(string columnName, string postgresTypeName)
    {
        var normalized = postgresTypeName.Trim().ToLowerInvariant();

        // numeric(p,s) carries its precision and scale in the type name (Npgsql reports e.g. "numeric(18,2)"), and
        // is exported as the exact DECIMAL(p,s) so megabyte figures keep their cents.
        var modified = ModifiedNumeric().Match(normalized);
        if (modified.Success)
        {
            var precision = int.Parse(modified.Groups[1].Value, CultureInfo.InvariantCulture);
            var scale = modified.Groups[2].Success ? int.Parse(modified.Groups[2].Value, CultureInfo.InvariantCulture) : 0;
            return precision <= MaxDuckDbDecimalPrecision
                ? string.Create(CultureInfo.InvariantCulture, $"DECIMAL({precision},{scale})")
                : "DOUBLE";
        }

        return normalized switch
        {
            "bool" or "boolean" => "BOOLEAN",
            "int2" or "smallint" => "SMALLINT",
            "int4" or "integer" => "INTEGER",
            "int8" or "bigint" => "BIGINT",
            "float4" or "real" => "REAL",
            "float8" or "double precision" => "DOUBLE",
            "text" or "varchar" or "character varying" or "char" or "character" or "name"
                or "json" or "jsonb" or "xml" => "VARCHAR",
            "bytea" => "BLOB",
            // Npgsql's GetDataTypeName reports the SQL-standard spelling for timestamp
            // columns ("timestamp without time zone" / "timestamp with time zone"), not the
            // short pg_type alias, so both spellings are accepted here.
            "timestamp" or "timestamp without time zone" => "TIMESTAMP",
            "timestamptz" or "timestamp with time zone" => "TIMESTAMPTZ",
            "date" => "DATE",
            "uuid" => "UUID",
            // Unconstrained numeric has no fixed precision, so it maps to DOUBLE. Exact integer sums are cast to
            // bigint in the SQL that produces them (SqlBuilder's hourly rollups cast every sum(...) to ::bigint).
            "numeric" or "decimal" => "DOUBLE",
            _ => throw new NotSupportedException(
                $"Column '{columnName}' has Postgres type '{postgresTypeName}', which has no DuckDB mapping. " +
                "Add one to DuckDbTypeMapper.Map or cast it in the store query."),
        };
    }
}
