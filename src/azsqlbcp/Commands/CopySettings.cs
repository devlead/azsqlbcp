using Spectre.Console;
using Spectre.Console.Cli;

namespace AzSqlBcp.Commands;

public sealed class CopySettings : CommandSettings
{
    [CommandOption("--source-server <SERVER>")]
    public required string SourceServer { get; init; }

    [CommandOption("--source-database <DATABASE>")]
    public required string SourceDatabase { get; init; }

    [CommandOption("--source-table <TABLE>")]
    public required string SourceTable { get; init; }

    [CommandOption("--source-port <PORT>")]
    public int SourcePort { get; init; } = 1433;

    [CommandOption("--source-trust-server-certificate")]
    public bool SourceTrustServerCertificate { get; init; }

    [CommandOption("--target-server <SERVER>")]
    public required string TargetServer { get; init; }

    [CommandOption("--target-database <DATABASE>")]
    public required string TargetDatabase { get; init; }

    [CommandOption("--target-table <TABLE>")]
    public required string TargetTable { get; init; }

    [CommandOption("--target-port <PORT>")]
    public int TargetPort { get; init; } = 1433;

    [CommandOption("--target-trust-server-certificate")]
    public bool TargetTrustServerCertificate { get; init; }

    [CommandOption("--partition-column <COLUMN>")]
    public string? PartitionColumn { get; init; }

    [CommandOption("--no-partition-column")]
    public bool NoPartitionColumn { get; init; }

    [CommandOption("--source-read-only")]
    public bool SourceReadOnly { get; init; }

    [CommandOption("--parallelism <N>")]
    public int Parallelism { get; init; } = 6;

    [CommandOption("--batch-size <N>")]
    public int BatchSize { get; init; } = 2_000_000;

    public override ValidationResult Validate()
    {
        if (string.IsNullOrWhiteSpace(SourceServer))
            return ValidationResult.Error("--source-server is required.");
        if (string.IsNullOrWhiteSpace(SourceDatabase))
            return ValidationResult.Error("--source-database is required.");
        if (string.IsNullOrWhiteSpace(SourceTable))
            return ValidationResult.Error("--source-table is required.");
        if (string.IsNullOrWhiteSpace(TargetServer))
            return ValidationResult.Error("--target-server is required.");
        if (string.IsNullOrWhiteSpace(TargetDatabase))
            return ValidationResult.Error("--target-database is required.");
        if (string.IsNullOrWhiteSpace(TargetTable))
            return ValidationResult.Error("--target-table is required.");

        if (NoPartitionColumn)
        {
            if (!string.IsNullOrWhiteSpace(PartitionColumn))
                return ValidationResult.Error("Do not pass --partition-column with --no-partition-column.");
        }
        else if (string.IsNullOrWhiteSpace(PartitionColumn))
        {
            return ValidationResult.Error("--partition-column is required (or use --no-partition-column).");
        }

        if (Parallelism < 1)
            return ValidationResult.Error("--parallelism must be >= 1.");
        if (BatchSize < 1)
            return ValidationResult.Error("--batch-size must be >= 1.");
        if (SourcePort is < 1 or > 65535)
            return ValidationResult.Error("--source-port must be between 1 and 65535.");
        if (TargetPort is < 1 or > 65535)
            return ValidationResult.Error("--target-port must be between 1 and 65535.");

        return ValidationResult.Success();
    }
}
