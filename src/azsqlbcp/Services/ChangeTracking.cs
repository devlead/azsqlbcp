using System.Data;
using Microsoft.Data.SqlClient;

namespace AzSqlBcp.Services;

public static class ChangeTracking
{
    public static async Task<ChangeTrackingInfo> CaptureBaselineAsync(
        string server,
        string database,
        string sourceTable,
        string accessToken,
        int port,
        bool readOnlyIntent,
        bool trustServerCertificate,
        CancellationToken cancellationToken)
    {
        ParseTable(sourceTable, out var schema, out var table);

        await using var conn = BulkCopyService.CreateConnection(
            server, database, accessToken, port, readOnlyIntent, trustServerCertificate);
        await conn.OpenAsync(cancellationToken).ConfigureAwait(false);

        if (!await IsDatabaseChangeTrackingEnabledAsync(conn, cancellationToken).ConfigureAwait(false))
            return new ChangeTrackingInfo { Enabled = false };

        if (!await IsTableChangeTrackingEnabledAsync(conn, schema, table, cancellationToken).ConfigureAwait(false))
            return new ChangeTrackingInfo { Enabled = false };

        await using var cmd = new SqlCommand("SELECT CHANGE_TRACKING_CURRENT_VERSION()", conn)
        {
            CommandType = CommandType.Text,
            CommandTimeout = 500000
        };

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        if (result is null or DBNull)
            return new ChangeTrackingInfo { Enabled = false };

        return new ChangeTrackingInfo
        {
            Enabled = true,
            SyncStartVersion = Convert.ToInt64(result),
            CapturedUtc = DateTimeOffset.UtcNow
        };
    }

    private static async Task<bool> IsDatabaseChangeTrackingEnabledAsync(
        SqlConnection conn,
        CancellationToken cancellationToken)
    {
        await using var cmd = new SqlCommand(
            """
            SELECT CASE
                WHEN EXISTS (SELECT 1 FROM sys.change_tracking_databases WHERE database_id = DB_ID()) THEN 1
                ELSE 0
            END
            """,
            conn)
        {
            CommandType = CommandType.Text,
            CommandTimeout = 500000
        };

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is int enabled && enabled == 1;
    }

    private static async Task<bool> IsTableChangeTrackingEnabledAsync(
        SqlConnection conn,
        string schema,
        string table,
        CancellationToken cancellationToken)
    {
        await using var cmd = new SqlCommand(
            """
            SELECT CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM sys.change_tracking_tables ct
                    INNER JOIN sys.tables t ON ct.object_id = t.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = @schema AND t.name = @table
                ) THEN 1
                ELSE 0
            END
            """,
            conn)
        {
            CommandType = CommandType.Text,
            CommandTimeout = 500000
        };

        cmd.Parameters.AddWithValue("@schema", schema);
        cmd.Parameters.AddWithValue("@table", table);

        var result = await cmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is int enabled && enabled == 1;
    }

    private static void ParseTable(string table, out string schema, out string name)
    {
        var parts = table.Split('.', 2, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 2)
        {
            schema = parts[0];
            name = parts[1];
            return;
        }

        schema = "dbo";
        name = parts[0];
    }
}
