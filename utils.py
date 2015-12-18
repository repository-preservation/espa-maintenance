import smtplib
from email.mime.text import MIMEText


CFG_NFO = "/home/{0}/.cfgnfo"

def get_cfg(user):
    """
    Retrieve the configuration information from the .cfgnfo file
    Will need to be made more robust if the file changes

    :return: dict
    """
    cfg_info = {}
    with open(CFG_NFO.format(user), 'r') as f:
        for line in f:
            spl = line.split('=')

            if len(spl) > 1:
                cfg_info[spl[0]] = line[len(spl[0]) + 1:]

    return cfg_info


def send_email(sender, recipient, subject, body):
    """
    Send out an email to give notice of success or failure

    :param sender: who the email is from
    :type sender: string
    :param recipient: list of recipients of the email
    :type recipient: list
    :param subject: subject line of the email
    :type subject: string
    :param body: success or failure message to be passed
    :type body: string
    :return:
    """
    # This does not need to be anything fancy as it is used internally,
    # as long as we can see if the script succeeded or where it failed
    # at, then we are good to go
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipient)

    smtp = smtplib.SMTP("localhost")
    smtp.sendmail(sender, recipient, msg.as_string())
    smtp.quit()
