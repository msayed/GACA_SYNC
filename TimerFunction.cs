using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace GACASYNC;

public class TimerFunction
{
    private readonly ILogger<TimerFunction> _logger;
    private readonly IGacaSyncService _gacaSyncService;

    public TimerFunction(ILogger<TimerFunction> logger, IGacaSyncService gacaSyncService)
    {
        _logger = logger;
        _gacaSyncService = gacaSyncService;
    }

    [Function(nameof(TimerFunction))]
    public async Task RunAsync(
        [TimerTrigger("0 0 */1 * * *", RunOnStartup = false)] TimerInfo timerInfo,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("Timer trigger started at {Timestamp}. IsPastDue: {IsPastDue}", DateTimeOffset.UtcNow, timerInfo.IsPastDue);

        await _gacaSyncService.RunOnceAsync(cancellationToken);

        _logger.LogInformation("Timer trigger completed at {Timestamp}", DateTimeOffset.UtcNow);
    }
}
