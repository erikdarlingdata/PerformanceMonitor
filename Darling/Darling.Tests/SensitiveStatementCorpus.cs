/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;

namespace Darling.Tests;

/// <summary>
/// The statement corpus shared by the .NET evaluation tests and the PostgreSQL parity test (#4348): what the
/// filter names, what it leaves alone, and the two strings only the .NET engine bounds with a time limit.
/// </summary>
internal static class SensitiveStatementCorpus
{
    /// <summary>Statements the filter must name. Each is also judged lower-cased and with a leading block
    /// comment (<see cref="Variants"/>).</summary>
    public static readonly string[] Named =
    {
        "CREATE LOGIN [app] WITH PASSWORD = N'S3cret'",
        "ALTER LOGIN [app] WITH PASSWORD = 0x0200AB HASHED",
        "ALTER LOGIN [app] DISABLE",
        "CREATE/* x */LOGIN [a] WITH PASSWORD = 'p'",
        "CREATE\r\nLOGIN [a] FROM WINDOWS",
        "EXEC sp_addlogin 'app', 'S3cret'",
        "EXEC master..sp_password NULL, 'S3cret', 'app'",
        "EXEC sp_addlinkedsrvlogin 'SRV', 'false', NULL, 'u', 'S3cret'",
        "EXEC sp_setapprole 'r', 'S3cret'",
        "OPEN SYMMETRIC KEY k DECRYPTION BY PASSWORD = 'S3cret'",
        "OPEN MASTER KEY DECRYPTION BY PASSWORD = N'S3cret'",
        "CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'S3cret'",
        "CREATE CREDENTIAL c WITH IDENTITY = 'u', SECRET = 'S3cret'",
        "CREATE DATABASE SCOPED CREDENTIAL c WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = 'sv=1&sig=x'",
        "BACKUP CERTIFICATE c TO FILE = 'f' WITH PRIVATE KEY (FILE = 'k', ENCRYPTION BY PASSWORD = 'S3cret')",
        "RESTORE DATABASE d FROM DISK = 'f' WITH MEDIAPASSWORD = 'S3cret'",
        "CREATE SYMMETRIC KEY k WITH KEY_SOURCE = 'phrase', ALGORITHM = AES_256 ENCRYPTION BY CERTIFICATE c",
        "SELECT * FROM OPENROWSET('MSOLEDBSQL', 'Server=h;UID=u;PWD=S3cret;', 'SELECT 1')",
        "SELECT * FROM OPENDATASOURCE('MSOLEDBSQL', 'Data Source=h;User ID=u;Pwd=S3cret').db.dbo.t",
        "EXEC sp_addlinkedserver @server = 'S', @provider = 'MSOLEDBSQL', @provstr = 'UID=u;PWD=S3cret'",
        "EXEC sp_addpushsubscription_agent @publication = N'p', @job_password = N'S3cret'",
        "ALTER SERVICE MASTER KEY WITH OLD_ACCOUNT = 'a', OLD_PASSWORD = 'S3cret'",
        "ALTER APPLICATION ROLE r WITH PASSWORD = N'S3cret'",
        "SELECT DECRYPTBYPASSPHRASE('phrase', @c)",
        "exec sp_executesql N'UPDATE dbo.t SET c = @pwd', N'@pwd nvarchar(50)', @pwd = N'S3cret'",
        "UPDATE dbo.Creds SET [password] = N'S3cret'",
        "UPDATE t SET \"password\" = 'S3cret'",
        "UPDATE dbo.Users SET pwd = N'S3cret' WHERE id = 7",
        "UPDATE dbo.Users SET password = @p",
        "SELECT 1 WHERE client_secret = 'S3cret'",

        // a typed declaration of a password variable (#5320): the name, a type with an optional length, then the literal
        "DECLARE @mypassword nvarchar(20) = N'S3cret'",
        "DECLARE @pwd varchar(max) = 'S3cret'",
        "DECLARE @secret sysname = N'S3cret'",
        "DECLARE @a int = 1, @pwd varchar(10) = 'S3cret'",
        "DECLARE @a int = 1, @b nvarchar(20) = N'x', @db_passwd nvarchar(20) = N'S3cret', @c int",
        "DECLARE @mypassword varbinary(64) = 0x0200AB",
        "DECLARE @mypassword AS nvarchar(20) = N'S3cret'",
        "DECLARE @mypassword [nvarchar](20) = N'S3cret'",
        "DECLARE @mypassword sys.sysname = N'S3cret'",
        "DECLARE @mypassword nvarchar (20) = N'S3cret'",
        "DECLARE @mypassword decimal(10, 2) = N'S3cret'",
        "DECLARE @pwd nvarchar(20) /* x */ = -- y\r\n N'S3cret'",
        "CREATE PROCEDURE dbo.p @password nvarchar(20) = N'S3cret' AS SELECT 1",
        "exec sp_executesql N'SELECT 1', N'@password nvarchar(20) = N''S3cret'''",
        "DECLARE pwd text := 'secret-x'; BEGIN NULL; END",
        "DECLARE db_secret varchar(20) = 'secret-x'",
        "DECLARE api_secret constant text := e'secret-x'",
        "DECLARE pwd text DEFAULT 'secret-x'",
        "DECLARE pwd text := $q$secret-x$q$",

        // the PostgreSQL forms
        "ALTER ROLE app PASSWORD 'secret-x'",
        "CREATE ROLE r LOGIN PASSWORD 'secret-x'",
        "CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'u', password 'secret-x')",
        "ALTER SYSTEM SET primary_conninfo = 'host=h password=secret-x'",
        "CREATE SUBSCRIPTION sub CONNECTION 'host=h password=secret-x' PUBLICATION p",
        "ALTER ROLE app SET work_mem = '64MB'",
        "CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'u', secret_access_key 'secret-x')",
        "ALTER SERVER s OPTIONS (ADD token 'secret-x')",
        "CREATE SERVER s FOREIGN DATA WRAPPER w OPTIONS (api_key 'secret-x')",
    };

    /// <summary>Statements the filter must leave alone.</summary>
    public static readonly string[] NotNamed =
    {
        "SELECT name FROM sys.sql_logins",
        "EXECUTE AS LOGIN = N'app'",
        "SELECT * FROM dbo.Users WHERE PasswordHash = @h",
        "UPDATE dbo.Users SET PasswordHash = HASHBYTES('SHA2_256', @p)",
        "SELECT * FROM OPENROWSET(BULK N'f.json', SINGLE_CLOB) AS j",
        "EXEC sp_helplogins",
        "SELECT secret_id FROM dbo.t WHERE secret_id = 5",
        "SELECT * FROM t WHERE pwd = @p",
        "SELECT * FROM t WHERE pwd = $1",

        // typed declarations that carry no literal, and neighbours of the typed-declaration shape (#5320)
        "DECLARE @password nvarchar(20) = @p",
        "DECLARE @mypassword nvarchar(20)",
        "DECLARE pwd text := $1",
        "CREATE PROCEDURE dbo.p @password nvarchar(20) = NULL AS SELECT 1",
        "CREATE TABLE dbo.u (id int, password_hash varbinary(64) NOT NULL)",
        "SELECT * FROM t WHERE pwd_len int",
        "DECLARE @pwd_len int = 8",
        "DECLARE @pwd int = 8",
        "SELECT password varchar FROM t WHERE x = 'a'",
        "SELECT * FROM t WHERE is_secret AND state = 'x'",
        "SELECT [password] FROM t",
        "SET password_encryption = 'scram-sha-256'",
        "ALTER INDEX ix ON dbo.t REBUILD",
        "BACKUP DATABASE d TO URL = 'https://storage.example/c/d.bak' WITH CREDENTIAL = 'cred'",
        "ALTER AVAILABILITY GROUP ag FAILOVER",

        // the PostgreSQL neighbours
        "SELECT * FROM t WHERE password_changed_at > $1",
        "SET work_mem = '64MB'",
        "SELECT rolname FROM pg_roles",
        "SELECT 1",
        "CREATE TABLE passwords (id int)",
    };

    /// <summary>The strings the plan once expected the .NET judge to time out on: many comment tokens after a
    /// keyword. The linear-time pre-check (#5320) now answers Clean for them, as PostgreSQL does. They are not
    /// part of the parity set, because the full .NET judge alone still backtracks on them.</summary>
    public static readonly string[] Adversarial =
    {
        "create" + string.Concat(Enumerable.Repeat(" --", 40)) + "x",
        "password" + string.Concat(Enumerable.Repeat(" --", 40)) + "x",
    };

    /// <summary>Strings aimed at the typed-declaration alternative (#5320): a password word, then many comment
    /// tokens, a type and a long length part, but no literal. The pre-check answers Clean for each of them. Kept
    /// apart from <see cref="Adversarial"/>, which the output-filter tests prefix with a pre-check hit so the
    /// full judge runs and times out; these do not make the full judge slow.</summary>
    public static readonly string[] TypedDeclarationAdversarial =
    {
        "pwd" + string.Concat(Enumerable.Repeat(" --", 40)) + " nvarchar(20) = x",
        "secret" + string.Concat(Enumerable.Repeat(" /* c */", 40)) + " text :",
        "pwd varchar(" + new string(' ', 100_000) + "x",
        "declare @pwd " + string.Concat(Enumerable.Repeat("nvarchar ", 5_000)) + "= @p",
    };

    /// <summary>A named statement as written, lower-cased, and with a leading block comment.</summary>
    public static string[] Variants(string text) => new[] { text, text.ToLowerInvariant(), "/* c */ " + text };
}
