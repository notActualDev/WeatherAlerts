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

        try
        {
            await client.SendAsync(Azure.WaitUntil.Started, message);
            logger.LogInformation("MAIL SENT OK via Azure to {To}", to);
            return new EmailSendResult.Success();
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "AZURE EMAIL SEND FAILED to {To}", to);
            return new EmailSendResult.Failed(ex.Message);
        }
    }
}
