using System.Net;
using System.Net.Mail;
using Microsoft.Extensions.Configuration;
using Azure;
using Azure.Communication.Email;
using Microsoft.Extensions.Logging;

public interface IEmailService
{
    Task<EmailSendResult> SendAsync(string to, string subject, string body);
}

public abstract record EmailSendResult
{
    private EmailSendResult() { }
    public sealed record Success : EmailSendResult;
    public sealed record Failed(string Reason) : EmailSendResult;
}

public sealed class AzureEmailService : IEmailService
{
    private const int EmailSendMaxAttempts = 3;

    private readonly EmailClient client;
    private readonly string senderAddress;
    private readonly ILogger<AzureEmailService> logger;

    public AzureEmailService(IConfiguration configuration, ILogger<AzureEmailService> logger)
    {
        var connectionString = configuration["AzureMail:ConnectionString"];
        if (string.IsNullOrWhiteSpace(connectionString)) throw new InvalidOperationException("Missing AzureMail:ConnectionString");
        this.senderAddress = configuration["AzureMail:SenderAddress"] ?? throw new InvalidOperationException("Missing AzureMail:SenderAddress");
        this.client = new EmailClient(connectionString);
        this.logger = logger;
    }

    public async Task<EmailSendResult> SendAsync(string to, string subject, string body)
    {
        var message = new EmailMessage(
            senderAddress: senderAddress,
            content: new EmailContent(subject)
            {
                PlainText = body,
                Html =
                    $"<div style=\"font-family:Segoe UI,Arial,sans-serif;white-space:pre-line\">{WebUtility.HtmlEncode(body)}</div>"
            },
            recipients: new EmailRecipients(new[] { new EmailAddress(to) }
            )
        );

        Exception? lastError = null;
        for (var attempt = 1; attempt <= EmailSendMaxAttempts; attempt++)
        {
            try
            {
                var operation = await client.SendAsync(Azure.WaitUntil.Completed, message);
                var result = operation.Value;

                if (result.Status == EmailSendStatus.Succeeded)
                {
                    logger.LogInformation(
                        "MAIL SENT OK via Azure to {To}. OperationId: {OperationId}, Status: {Status}",
                        to,
                        operation.Id,
                        result.Status);

                    return new EmailSendResult.Success();
                }

                lastError = new InvalidOperationException(
                    $"Email send status is {result.Status}.");

                logger.LogWarning(
                    "Azure email operation finished without success for {To}. OperationId: {OperationId}, Status: {Status}, Attempt: {Attempt}/{MaxAttempts}",
                    to,
                    operation.Id,
                    result.Status,
                    attempt,
                    EmailSendMaxAttempts);
            }
            catch (Exception ex) when (attempt < EmailSendMaxAttempts && IsTransientEmailException(ex))
            {
                lastError = ex;
                var delay = TimeSpan.FromSeconds(attempt * 2);
                logger.LogWarning(
                    ex,
                    "Transient Azure email failure for {To}, retrying in {DelaySeconds}s (attempt {Attempt}/{MaxAttempts}).",
                    to,
                    delay.TotalSeconds,
                    attempt + 1,
                    EmailSendMaxAttempts);
                await Task.Delay(delay);
                continue;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "AZURE EMAIL SEND FAILED to {To}", to);
                return new EmailSendResult.Failed(ex.Message);
            }

            if (attempt < EmailSendMaxAttempts)
            {
                var delay = TimeSpan.FromSeconds(attempt * 2);
                await Task.Delay(delay);
            }
        }

        logger.LogError(lastError, "AZURE EMAIL SEND FAILED to {To} after retries", to);
        return new EmailSendResult.Failed(lastError?.Message ?? "Email send failed.");
    }

    private static bool IsTransientEmailException(Exception ex)
    {
        if (ex is TimeoutException)
        {
            return true;
        }

        if (ex is RequestFailedException requestFailed)
        {
            return requestFailed.Status == 408
                || requestFailed.Status == 429
                || requestFailed.Status == 500
                || requestFailed.Status == 502
                || requestFailed.Status == 503
                || requestFailed.Status == 504;
        }

        if (ex.InnerException is not null)
        {
            return IsTransientEmailException(ex.InnerException);
        }

        return false;
    }
}
