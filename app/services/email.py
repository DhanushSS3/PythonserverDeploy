# app/services/email.py

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from app.core.config import get_settings # Import settings

settings = get_settings()

async def send_email(to_email: str, subject: str, body: str):
    """
    Sends an email using the configured SMTP settings.

    Args:
        to_email: The recipient's email address.
        subject: The subject of the email.
        body: The body of the email (plain text or HTML).
    """
    # Create a multipart message
    msg = MIMEMultipart()
    msg['From'] = settings.DEFAULT_FROM_EMAIL
    msg['To'] = to_email
    msg['Subject'] = subject

    # Attach the body with MIMEText
    msg.attach(MIMEText(body, 'plain')) # Can change 'plain' to 'html' if sending HTML emails

    try:
        # Connect to the SMTP server
        # Use SSL if EMAIL_USE_SSL is True
        if settings.EMAIL_USE_SSL:
            server = smtplib.SMTP_SSL(settings.EMAIL_HOST, settings.EMAIL_PORT)
        else:
            server = smtplib.SMTP(settings.EMAIL_HOST, settings.EMAIL_PORT)
            # server.starttls() # Uncomment if using STARTTLS

        # Login to the SMTP server
        server.login(settings.EMAIL_HOST_USER, settings.EMAIL_HOST_PASSWORD)

        # Send the email
        server.sendmail(settings.DEFAULT_FROM_EMAIL, to_email, msg.as_bytes())

        # Disconnect from the server
        server.quit()
        print(f"Email sent successfully to {to_email}") # Log success
    except Exception as e:
        print(f"Error sending email to {to_email}: {e}") # Log the error
        # In a real application, you might want to raise this exception
        # or handle it more gracefully (e.g., queue the email for retry).
        raise # Re-raise the exception for now
