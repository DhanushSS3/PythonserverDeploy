"""
HTML Email Templates for LiveFXHub
Professional email templates with proper styling and branding
"""

def get_margin_call_email_template():
    """
    Returns HTML template for margin call warning emails
    Use this template with your email sending service
    """
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Margin Call Warning - LiveFXHub</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            color: #333333;
            background-color: #f8fafc;
        }}
        
        .email-container {{
            max-width: 600px;
            margin: 0 auto;
            background-color: #ffffff;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }}
        
        .header {{
            background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
            padding: 10px;
            text-align: center;
            border-radius: 8px 8px 0 0;
        }}
        
        .logo {{
            font-size: 24px;
            font-weight: 600;
            color: #ffffff;
            letter-spacing: -1px;   
            margin-bottom: 0px;
        }}
        
        .logo .fx {{
            color: #3b82f6;
        }}
        
        .tagline {{
            color: #94a3b8;
            font-size: 14px;
            font-weight: 400;
        }}
        
        .alert-banner {{
            background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%);
            color: white;
            padding: 15px 20px;
            text-align: center;
            font-weight: 600;
            font-size: 16px;
            border-left: 4px solid #fbbf24;
        }}
        
        .content {{
            padding:  20px;
        }}
        
        .greeting {{
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 15px;
            color: #1e293b;
        }}
        
        .warning-box {{
            background: linear-gradient(135deg, #fef3c7 0%, #fde68a 100%);
            border: 2px solid #f59e0b;
            border-radius: 8px;
            padding: 20px;
            margin: 25px 0;
            position: relative;
        }}
        
        .warning-icon {{
            position: absolute;
            top: -10px;
            left: 20px;
            background: #f59e0b;
            color: white;
            width: 20px;
            height: 20px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: bold;
            font-size: 12px;
        }}
        
        .margin-details {{
            background: #f8fafc;
            border-radius: 8px;
            padding: 20px;
            margin: 20px 0;
            border-left: 4px solid #3b82f6;
        }}
        
        .margin-level {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }}
        
        .level-label {{
            font-weight: 600;
            color: #475569;
        }}
        
        .level-value {{
            font-size: 24px;
            font-weight: 700;
            color: #ef4444;
        }}
        
        .progress-bar {{
            width: 100%;
            height: 8px;
            background: #e2e8f0;
            border-radius: 4px;
            overflow: hidden;
            margin-bottom: 10px;
        }}
        
        .progress-fill {{
            height: 100%;
            background: linear-gradient(90deg, #ef4444 0%, #dc2626 100%);
            width: 85%;
            border-radius: 4px;
        }}
        
        .threshold-info {{
            font-size: 14px;
            color: #64748b;
            text-align: center;
        }}
        
        .action-required {{
            background: #fee2e2;
            border: 1px solid #fecaca;
            border-radius: 8px;
            padding: 20px;
            margin: 25px 0;
            text-align: center;
        }}
        
        .action-title {{
            font-size: 18px;
            font-weight: 700;
            color: #dc2626;
            margin-bottom: 10px;
        }}
        
        .cta-button {{
            display: inline-block;
            background: linear-gradient(135deg, #3b82f6 0%, #2563eb 100%);
            color: white;
            padding: 12px 30px;
            text-decoration: none;
            border-radius: 6px;
            font-weight: 600;
            margin: 20px 0 10px 0;
            box-shadow: 0 2px 4px rgba(59, 130, 246, 0.3);
        }}
        
        .cta-button:hover {{
            background: linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%);
        }}
        
        .footer {{
            background: #f8fafc;
            padding: 30px 20px;
            text-align: center;
            border-top: 1px solid #e2e8f0;
        }}
        
        .footer-content {{
            color: #64748b;
            font-size: 14px;
            line-height: 1.5;
        }}
        
        .company-info {{
            margin-top: 20px;
            padding-top: 20px;
            border-top: 1px solid #e2e8f0;
        }}
        
        .social-links {{
            margin: 15px 0;
        }}
        
        .social-links a {{
            color: #64748b;
            text-decoration: none;
            margin: 0 10px;
        }}
        
        @media only screen and (max-width: 600px) {{
            .email-container {{
                margin: 10px;
                border-radius: 8px;
            }}
            
            .content {{
                padding: 25px 20px;
            }}
            
            .logo {{
                font-size: 28px;
            }}
            
            .level-value {{
                font-size: 20px;
            }}
        }}
    </style>
</head>
<body>
    <div class="email-container">
        <!-- Header with Logo -->
        <div class="header">
            <div class="logo">Live<span class="fx">FX</span>Hub</div>
            
        </div>
        
        <!-- Alert Banner -->
        <div class="alert-banner">
            ⚠️ MARGIN CALL WARNING: IMMEDIATE ACTION REQUIRED
        </div>
        
        <!-- Main Content -->
        <div class="content">
            <div class="greeting">Dear Trader,</div>
            
            <!-- Warning Box -->
            <div class="warning-box">
                <div class="warning-icon">!</div>
                <p><strong>Your account margin level has fallen below the margin call threshold of 70%.</strong></p>
            </div>
            
            <!-- Margin Details -->
            <div class="margin-details">
                <div class="margin-level">
                    <span class="level-label">Current Margin Level:</span>
                    <span class="level-value">{margin_level}%</span>
                </div>
                <div class="progress-bar">
                    <div class="progress-fill"></div>
                </div>
                
            </div>
            
            <p style="margin-bottom: 20px;">
                If your margin level drops below <strong style="color: #ef4444;">10.0%</strong>, your open positions 
                may be automatically closed (auto-cutoff) to protect your account from further losses.
            </p>
            
            <!-- Action Required Section -->
            <div class="action-required">
                <div class="action-title">Immediate Action Required</div>
                <p>Please review your positions and take appropriate action to avoid auto-cutoff.</p>
                <a href="{dashboard_url}" class="cta-button">Access Your Dashboard</a>
                <p style="font-size: 12px; margin-top: 10px; color: #64748b;">
                    Log in to your account to manage your positions
                </p>
            </div>
            
            <p style="margin-top: 25px;">
                <strong>Recommended Actions:</strong>
            </p>
            <ul style="margin: 15px 0 25px 20px; color: #475569;">
                <li>Close some positions to reduce margin usage</li>
                <li>Deposit additional funds to your account</li>
                <li>Review and adjust your risk management strategy</li>
            </ul>
            
            <p style="margin-top: 30px; font-weight: 600;">
                Thank you,<br>
                <span style="color: #3b82f6;">LiveFXHub Trading Platform Support Team</span>
            </p>
        </div>
        
        <!-- Footer -->
        <div class="footer">
            <div class="footer-content">
                <strong>LiveFXHub</strong>
                <br>
                This is an automated message. Please do not reply to this email.<br>
                For support, contact us at support@livefxhub.com
                
                <div class="company-info">
                    <div class="social-links">
                        <a href="#">Terms of Service</a> |
                        <a href="#">Privacy Policy</a> |
                        <a href="#">Risk Disclosure</a>
                    </div>
                    © 2024 LiveFXHub. All rights reserved.
                </div>
            </div>
        </div>
    </div>
</body>
</html>
"""

def get_general_notification_template():
    """
    Returns HTML template for general notifications
    """
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{subject} - LiveFXHub</title>
    <style>
        /* Same base styles as margin call template */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            line-height: 1.6;
            color: #333333;
            background-color: #f8fafc;
        }
        
        .email-container {
            max-width: 600px;
            margin: 0 auto;
            background-color: #ffffff;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        .header {
            background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
            padding: 30px 20px;
            text-align: center;
            border-radius: 8px 8px 0 0;
        }
        
        .logo {
            font-size: 32px;
            font-weight: 700;
            color: #ffffff;
            letter-spacing: -1px;
            margin-bottom: 8px;
        }
        
        .logo .fx {
            color: #3b82f6;
        }
        
        .content {
            padding: 40px 30px;
        }
        
        .footer {
            background: #f8fafc;
            padding: 30px 20px;
            text-align: center;
            border-top: 1px solid #e2e8f0;
            color: #64748b;
            font-size: 14px;
        }
    </style>
</head>
<body>
    <div class="email-container">
        <div class="header">
            <div class="logo">Live<span class="fx">FX</span>Hub</div>
        </div>
        
        <div class="content">
            {content}
        </div>
        
        <div class="footer">
            <strong>LiveFXHub</strong><br>
            Professional Trading Platform<br><br>
            © 2024 LiveFXHub. All rights reserved.
        </div>
    </div>
</body>
</html>
"""

# Usage example:
"""
# In your Python email sending code:
from email_template import get_margin_call_email_template

# Get the template
template = get_margin_call_email_template()

# Replace placeholders with actual values
email_html = template.format(
    margin_level="85.2",
    dashboard_url="https://your-dashboard-url.com"
)

# Send email using your preferred email service
# (SendGrid, AWS SES, etc.)
"""