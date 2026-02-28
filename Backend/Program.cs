var builder = WebApplication.CreateBuilder(args);

builder.Services.AddMemoryCache();
builder.Services.AddScoped<IEmailService, AzureEmailService>();
builder.Services.AddScoped<ISubscriptionRequestService, SubscriptionRequestService>();
builder.Services.AddScoped<IUserService, UserService>();
builder.Services.AddHttpClient<IAlertsService, AlertsService>();

builder.Services.AddCors(options =>
{
    options.AddDefaultPolicy(policy =>
    {
        policy
            .AllowAnyOrigin()
            .AllowAnyHeader()
            .AllowAnyMethod();
    });
});
var app = builder.Build();
app.UseCors();

app.MapGet("/", () => "ROOT OK");
app.MapGet("/health", () => Results.Ok("OK"));
app.MapGet("/api/ping", () => Results.Ok("pong"));
app.MapGet("/sleepless", async (IConfiguration configuration, CancellationToken ct) =>
{
    var connectionString = configuration.GetConnectionString("Default");
    if (string.IsNullOrWhiteSpace(connectionString))
    {
        return Results.Problem(
            title: "Configuration error",
            detail: "Missing connection string: Default",
            statusCode: StatusCodes.Status500InternalServerError);
    }

    var startedAtUtc = DateTime.UtcNow;

    try
    {
        await using var con = new Microsoft.Data.SqlClient.SqlConnection(connectionString);
        await con.OpenAsync(ct);

        await using var cmd = new Microsoft.Data.SqlClient.SqlCommand("SELECT 1;", con);
        var dbPing = await cmd.ExecuteScalarAsync(ct);

        var elapsedMs = (int)Math.Round((DateTime.UtcNow - startedAtUtc).TotalMilliseconds);
        return Results.Ok(new
        {
            App = "awake",
            Database = "awake",
            SelectResult = dbPing,
            ElapsedMs = elapsedMs
        });
    }
    catch (Exception ex)
    {
        return Results.Problem(
            title: "Sleepless check failed",
            detail: ex.Message,
            statusCode: StatusCodes.Status500InternalServerError);
    }
});
app.MapGet("/api/debug/send-test-mail", async (IEmailService emailService, IConfiguration configuration, ILogger<Program> logger) =>
{
    var debugEmail = configuration["DEBUG_EMAIL"];
    if (string.IsNullOrWhiteSpace(debugEmail))
    {
        return Results.Problem(
            title: "Debug email failed",
            detail: "Missing DEBUG_EMAIL configuration",
            statusCode: StatusCodes.Status500InternalServerError);
    }

    var result = await emailService.SendAsync(
        debugEmail,
        "WeatherAlerts DEBUG",
        "This is a debug mail from /api/debug/send-test-mail.");

    if (result is EmailSendResult.Success)
    {
        logger.LogInformation("Debug test mail sent.");
        return Results.Ok("Debug mail sent.");
    }

    if (result is EmailSendResult.Failed failed)
    {
        logger.LogWarning("Debug test mail failed: {Reason}", failed.Reason);
        return Results.Problem(
            title: "Debug mail failed",
            detail: failed.Reason,
            statusCode: StatusCodes.Status500InternalServerError);
    }

    return Results.StatusCode(StatusCodes.Status500InternalServerError);
});

app.MapPost("/api/requestSubscription", async (SubscriptionRequestDto request, ISubscriptionRequestService service, ILogger<Program> logger) =>
{
    var hasSubscriptionRequest = await service.HasActiveSubscriptionRequestAsync(request.Email);

    if (hasSubscriptionRequest is HasActiveSubscriptionRequestResult.Failed failedCheck)
        return Results.BadRequest(new { error = failedCheck.Reason });

    if (hasSubscriptionRequest is HasActiveSubscriptionRequestResult.NotExists)
    {
        var createResult = await service.CreateActiveSubscriptionRequestAsync(request.Email);
        if (createResult is SubscriptionRequestCreationResult.Failed failed)
            return Results.BadRequest(new { error = failed.Reason });
    }

    _ = service.SendSubscriptionConfirmationMailAsync(request.Email);
    return Results.Accepted();
});

app.MapPost("/api/requestSubscriptionSettings", async (SubscriptionSettingsRequestDto request, ISubscriptionRequestService service) =>
{
    if (string.IsNullOrWhiteSpace(request.Token) || !Guid.TryParse(request.Token, out var token))
    {
        return Results.BadRequest(new { error = "Invalid token" });
    }

    var hasActiveToken = await service.HasActiveSubscriptionTokenAsync(token);

    if (hasActiveToken is HasActiveSubscriptionTokenResult.Failed failedCheck)
        return Results.BadRequest(new { error = failedCheck.Reason });

    if (hasActiveToken is HasActiveSubscriptionTokenResult.Invalid)
        return Results.BadRequest(new { error = "Token does not exist or is not active" });

    return Results.Accepted();
});

app.MapPost("/api/getUsersCities",
async (TokenDto request, IUserService service) =>
{
    var result = await service.GetUserCitiesAsync(request);

    if (result is GetUserCitiesResult.InvalidToken)
        return Results.BadRequest(new { error = "Invalid token" });

    if (result is GetUserCitiesResult.Failed failed)
        return Results.BadRequest(new { error = failed.Reason });

    var success = result as GetUserCitiesResult.Success;
    return Results.Ok(success!.Locations);
});

app.MapPost("/api/confirmSubscription",
async (
    ConfirmSubscriptionDto request,
    ISubscriptionRequestService service,
    IAlertsService alertsService,
    ILogger<Program> logger) =>
{
    if (!Guid.TryParse(request.Token, out var token))
        return Results.BadRequest(new { error = "Invalid token" });

    var result = await service.ConfirmSubscriptionAsync(token, request.Locations);

    if (result is SubscriptionConfirmationResult.InvalidToken)
        return Results.BadRequest(new { error = "Invalid token" });

    if (result is SubscriptionConfirmationResult.Failed failed)
        return Results.BadRequest(new { error = failed.Reason });

    if (result is SubscriptionConfirmationResult.Confirmed confirmed &&
        confirmed.NewCityIds is { Count: > 0 })
    {
        var insertedAlerts = await alertsService.CreateAndStoreAlertsForCitiesAsync(confirmed.NewCityIds);
        logger.LogInformation(
            "Generated alerts for newly created cities after subscription confirmation. NewCities: {NewCitiesCount}, InsertedAlerts: {InsertedAlerts}",
            confirmed.NewCityIds.Count,
            insertedAlerts);
    }

    var emailResult = await service.GetSubscriptionRequestEmailByTokenAsync(token);
    if (emailResult is GetSubscriptionRequestEmailByTokenResult.Found found)
    {
        var sendResult = await alertsService.SendExistingAlertsToUserAsync(found.Email);
        if (sendResult is SendExistingAlertsToUserResult.Failed sendFailed)
        {
            logger.LogWarning(
                "Subscription confirmed for {Email}, but sending existing alerts failed: {Reason}",
                found.Email,
                sendFailed.Reason);
        }
    }

    return Results.Ok();
});

app.MapPost("/api/requestUnsubscription",
async (
    UnsubscriptionRequestDto request,
    ISubscriptionRequestService service
) =>
{
    if (string.IsNullOrWhiteSpace(request.Email))
        return Results.BadRequest(new { error = "Email required" });

    var result = await service.CreateUnsubscriptionRequestAsync(request.Email);

    if (result is SubscriptionRequestCreationResult.Failed failed)
        return Results.BadRequest(new { error = failed.Reason });

    _ = service.SendUnsubscriptionConfirmationMailAsync(request.Email);

    return Results.Accepted();
});

app.MapGet("/api/confirmUnsubscription",
async (string token, ISubscriptionRequestService service) =>
{
    if (!Guid.TryParse(token, out var parsedToken))
        return Results.BadRequest("Invalid token");

    var result = await service.ConfirmUnsubscriptionAsync(parsedToken);

    if (result is SubscriptionConfirmationResult.InvalidToken)
        return Results.BadRequest("Invalid or expired token");

    if (result is SubscriptionConfirmationResult.Failed failed)
        return Results.BadRequest(failed.Reason);

    return Results.Ok("You have been unsubscribed.");
});

app.MapPost("/createAlertsAndSend", async (
    CreateAlertsAndSendRequestDto request,
    IAlertsService alertsService,
    IEmailService emailService,
    IConfiguration configuration,
    ILogger<Program> logger,
    CancellationToken ct) =>
{
    var debugEmail = configuration["DEBUG_EMAIL"];

    async Task SendDebugMailAsync(string subject, string body)
    {
        if (string.IsNullOrWhiteSpace(debugEmail))
        {
            logger.LogWarning("DEBUG_EMAIL is missing. Debug mail not sent. Subject: {Subject}", subject);
            return;
        }

        await emailService.SendAsync(debugEmail, subject, body);
    }

    try
    {
        var expectedToken = configuration["CREATE_AND_SEND_ALERTS_PASSWORD"];
        if (string.IsNullOrWhiteSpace(expectedToken))
        {
            await SendDebugMailAsync(
                "DEBUG /createAlertsAndSend config error",
                "Missing CREATE_AND_SEND_ALERTS_PASSWORD environment variable.");

            return Results.Problem(
                title: "Configuration error",
                detail: "Missing CREATE_AND_SEND_ALERTS_PASSWORD",
                statusCode: StatusCodes.Status500InternalServerError);
        }

        var providedToken = request?.Token ?? string.Empty;
        if (string.IsNullOrWhiteSpace(providedToken) ||
            !CreateAndSendAuth.IsCreateAndSendTokenValid(providedToken, expectedToken))
        {
            await SendDebugMailAsync(
                "DEBUG /createAlertsAndSend unauthorized",
                $"Authorization failed.\nIncoming token: {providedToken}\nExpected token: {expectedToken}");

            return Results.Unauthorized();
        }

        var summary = await alertsService.CreateStoreAndSendAlertsAsync(ct);
        logger.LogInformation(
            "Alerts created and sent. Cities: {CitiesCount}, Forecasts: {ForecastsCount}, Generated: {Generated}, Inserted: {Inserted}, UsersWithAlerts: {UsersWithAlerts}, Sent: {Sent}, Failed: {Failed}",
            summary.CitiesCount,
            summary.ForecastsCount,
            summary.AlertsGeneratedCount,
            summary.AlertsInsertedCount,
            summary.UsersWithAlertsCount,
            summary.EmailsSentCount,
            summary.EmailsFailedCount);

        if (summary.EmailsFailedCount > 0)
        {
            await SendDebugMailAsync(
                "DEBUG /createAlertsAndSend partial failure",
                $"Pipeline completed with email send failures.\n" +
                $"Cities: {summary.CitiesCount}\n" +
                $"Forecasts: {summary.ForecastsCount}\n" +
                $"Alerts generated: {summary.AlertsGeneratedCount}\n" +
                $"Alerts inserted: {summary.AlertsInsertedCount}\n" +
                $"Users with alerts: {summary.UsersWithAlertsCount}\n" +
                $"Emails sent: {summary.EmailsSentCount}\n" +
                $"Emails failed: {summary.EmailsFailedCount}");
        }

        return Results.Ok(summary);
    }
    catch (Exception ex)
    {
        logger.LogError(ex, "Create and send alerts pipeline failed");
        await SendDebugMailAsync(
            "DEBUG /createAlertsAndSend exception",
            $"Exception: {ex.Message}\n\n{ex}");

        return Results.Problem(
            title: "Create and send alerts failed",
            detail: ex.Message,
            statusCode: StatusCodes.Status500InternalServerError);
    }
});

app.Run();
