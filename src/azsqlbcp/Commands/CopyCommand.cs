using Spectre.Console.Cli;

namespace AzSqlBcp.Commands;

public sealed class CopyCommand(BulkCopyService service) : AsyncCommand<CopySettings>
{
    public override async Task<int> ExecuteAsync(CommandContext context, CopySettings settings, CancellationToken cancellationToken)
    {
        return await service.CopyAsync(settings, cancellationToken).ConfigureAwait(false);
    }
}
