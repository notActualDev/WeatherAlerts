using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Company.Function;

public class CreateAlertsTimer
{
    private readonly ILogger _logger;
    private readonly IConfiguration _config;
    private readonly HttpClient _httpClient;

    public CreateAlertsTimer(
        ILoggerFactory loggerFactory,
        IConfiguration config,
        IHttpClientFactory httpClientFactory)
    {
        _logger = loggerFactory.CreateLogger<CreateAlertsTimer>();
        _config = config;
        _httpClient = httpClientFactory.CreateClient();
    }

    [Function("CreateAlertsTimer2")]
    public async Task Run(
        [TimerTrigger("0 0 3,15 * * *")] TimerInfo myTimer)
    {
        _logger.LogInformation("Timer started at {time}", DateTime.Now);

        var token = _config["CREATE_AND_SEND_ALERTS_PASSWORD"];
        var endpoint = _config["CREATE_ALERTS_ENDPOINT"];

        var dto = new
        {
            Token = token
        };

        var json = JsonSerializer.Serialize(dto);

        var response = await _httpClient.PostAsync(
            endpoint,
            new StringContent(json, Encoding.UTF8, "application/json"));

        _logger.LogInformation("Status: {status}", response.StatusCode);
    }
}
