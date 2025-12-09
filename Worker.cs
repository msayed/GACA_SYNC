using System;
using System.Data;
using System.Net;
using System.Net.Mail;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace GACASYNC
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly GacaRepository _repository;
        private readonly IConfiguration _configuration;
        private readonly IGacaSyncService _gacaSyncService;

        public Worker(
            ILogger<Worker> logger,
            GacaRepository repository,
            IConfiguration configuration,
            IGacaSyncService gacaSyncService)
        {
            _logger = logger;
            _repository = repository;
            _configuration = configuration;
            _gacaSyncService = gacaSyncService;
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("GacaSyncService is starting at {time}", DateTimeOffset.Now);
            return base.StartAsync(cancellationToken);
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("GacaSyncService is stopping at {time}", DateTimeOffset.Now);
            return base.StopAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            int intervalMinutes = _configuration.GetValue<int>("WorkerSettings:IntervalMinutes", 30);
            _logger.LogInformation("Worker running at: {time} with interval {interval} minutes", DateTimeOffset.Now, intervalMinutes);

            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("Starting process at: {time}", DateTimeOffset.Now);
                try
                {
                    _gacaSyncService.RunOnce(stoppingToken);
                    // Process_2(); // Uncomment if needed, logic is ported but commented out in original
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in process");
                    _gacaSyncService.SendEmail($"Error in process: {ex.Message}");
                }

                _logger.LogInformation("Process completed at: {time}", DateTimeOffset.Now);
                await Task.Delay(TimeSpan.FromMinutes(intervalMinutes), stoppingToken);
            }
        }


    }
}
