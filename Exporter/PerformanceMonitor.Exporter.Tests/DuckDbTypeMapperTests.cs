using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class DuckDbTypeMapperTests
{
    [Theory]
    [InlineData("bool", "BOOLEAN")]
    [InlineData("int2", "SMALLINT")]
    [InlineData("int4", "INTEGER")]
    [InlineData("int8", "BIGINT")]
    [InlineData("float4", "REAL")]
    [InlineData("float8", "DOUBLE")]
    [InlineData("text", "VARCHAR")]
    [InlineData("varchar", "VARCHAR")]
    [InlineData("char", "VARCHAR")]
    [InlineData("name", "VARCHAR")]
    [InlineData("json", "VARCHAR")]
    [InlineData("jsonb", "VARCHAR")]
    [InlineData("xml", "VARCHAR")]
    [InlineData("bytea", "BLOB")]
    [InlineData("timestamp", "TIMESTAMP")]
    [InlineData("timestamptz", "TIMESTAMPTZ")]
    [InlineData("date", "DATE")]
    [InlineData("uuid", "UUID")]
    [InlineData("numeric", "DOUBLE")]
    [InlineData("timestamp without time zone", "TIMESTAMP")]
    [InlineData("timestamp with time zone", "TIMESTAMPTZ")]
    [InlineData("character varying", "VARCHAR")]
    [InlineData("numeric(18,2)", "DECIMAL(18,2)")]
    [InlineData("numeric(18, 2)", "DECIMAL(18,2)")]
    [InlineData("numeric(10)", "DECIMAL(10,0)")]
    [InlineData("decimal(38,4)", "DECIMAL(38,4)")]
    [InlineData("numeric(50,2)", "DOUBLE")]
    public void Map_returns_the_documented_type(string pgType, string expected)
    {
        Assert.Equal(expected, DuckDbTypeMapper.Map("some_column", pgType));
    }

    [Fact]
    public void Map_fails_loudly_and_names_the_column_for_an_unknown_type()
    {
        var ex = Assert.Throws<NotSupportedException>(() => DuckDbTypeMapper.Map("weird_column", "interval"));
        Assert.Contains("weird_column", ex.Message);
        Assert.Contains("interval", ex.Message);
    }
}
