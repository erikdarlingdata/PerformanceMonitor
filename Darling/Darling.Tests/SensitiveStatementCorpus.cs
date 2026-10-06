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

    /// <summary>A named statement as written, lower-cased, and with a leading block comment.</summary>
    public static string[] Variants(string text) => new[] { text, text.ToLowerInvariant(), "/* c */ " + text };
}
