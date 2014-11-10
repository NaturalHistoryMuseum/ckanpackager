"""An SMTP server library designed specifically for running test suites.

Overview
--------

SMTPretty makes it easy to start and stop a local dummy SMTP server from within
a test suite, and perform tests and assertions on the sent emails. Usage
is straightforward:

    import smtpretty
    smtpretty.enable(2525)
    call_my_test_code({'smtp_host': '127.0.0.1', 'smtp_port': 2525})
    assert(len(smtpretty.messages) == 1)
    assert(smtpretty.last_message.mail_from == 'testfrom@example.com')
    assert(smtpretty.last_message.recipients == ['testto@example.com'])
    assert(smtpretty.last_message.body == 'test email')
    smtpretty.disable()

You can also use a decorator to enable/disable automatically:

    import smtppretty
    @smtpretty.activate(2525)
    def test_something():
        call_my_test_code({'smtp_host': '127.0.0.1', 'smtp_port': 2525})
        assert(len(smtpretty.messages) == 1)
        assert(smtpretty.last_message.mail_from == 'testfrom@example.com')

Caveats and other notes
-----------------------

- SMTPretty runs an actual server. This means that in order to run it on port
  25, you will need to run it as root. A better approach is to ensure your code
  can be configured to use an SMTP server on another port. Future versions of
  SMTPretty might use a different approach (by patching the socket module,
  as [HTTPretty](https://github.com/gabrielfalcao/HTTPretty) does, though there
  is no definite plans at the moment);

- SMTPretty does not currently allow to test for email sending failures;

- SMTPretty can only run one SMTP server instance at a time, which covers the
  vast majority of use cases, and allows for a straightforward API.


smtpretty.messages
------------------
`smtpretty.messages` contains the list of all messages sent since smtpretty
was last activated.

Each message is an instance of a SMTPrettyEmail object, and has the
following attributes:

- mail_from : String containing the envelope originator
- recipients: List of strings defining the envelope recipients
- headers : Dictionary of (lower-cased) header to list of values
- body : The message body of the first part of the email. This provided as
  convenience for testing simple messages
- raw_message : String containing the message as received
- message : An [email.message](https://docs.python.org/2/library/email.message.html)
            object representing the message.

For convenience, `smtpretty.last_message` contains the last sent message.

Acknowledgements
----------------
SMTPretty was inspired by this blog post:
https://muffinresearch.co.uk/fake-smtp-server-with-python/
and while it does not use the same approach (it runs an actual server rather
than patching sockets), the API was inspired by
[HTTPretty](https://github.com/gabrielfalcao/HTTPretty)
"""
import smtpd
import threading
import asyncore
from functools import wraps
from email import message_from_string


messages = []
last_message = None
_server = None
_thread = None


def enable(port=25, bind='127.0.0.1'):
    """Enable SMTPretty by starting an SMTP server.

    Parameters
    ----------
    bind : str
        IP address to bind to
    port : int
        Port to bind to
    """
    global _server, _thread, messages
    if _server is not None:
        disable()
    messages = []
    last_message = None
    _server = SMTPrettyServer((bind, port), None)
    # Thanks to http://stackoverflow.com/questions/14483195/how-to-handle-asyncore-within-a-class-in-python-without-blocking-anything
    _thread = threading.Thread(target=asyncore.loop, kwargs={'timeout':1})
    _thread.start()


def disable():
    """Disables SMTPretty by stopping the SMTP server"""
    global _server, _thread
    if _server is not None:
        _server.close()
        _thread.join()
    _server = None
    _thread = None


def activate(method_or_port=25, bind='127.0.0.1'):
    """Decorator to enable SMTPretty before entry, and disable it after leaving

    Note that this can be used as a simple decorator (@activate) or as a
    parametrized decorator (@activate(port=2525))
    """
    port = method_or_port
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kargs):
            enable(port, bind)
            try:
                return f(*args, **kargs)
            finally:
                disable()
        return wrapper
    # Check whether we were called as a decorator (@activate) or as a function
    # that returns a decorator (@activate(2525))
    if callable(method_or_port):
        port = 25
        return decorator(method_or_port)
    else:
        return decorator


class SMTPrettyServer(smtpd.SMTPServer):
    """The SMTP server used to receive the sent messages"""
    def process_message(self, peer, mail_from, recipients, raw_message):
        global messages, last_message
        last_message = SMTPrettyEmail(mail_from, recipients, raw_message)
        messages.append(last_message)


class SMTPrettyEmail(object):
    """This represents the emails received by SMTPretty

    Attributes
    ----------
    mail_from : str
        The envelope originator
    recipients : list of str
        The envelope recipients
    headers : dict
        A dictionary of (lower-cased) headers to values
    body : str
        The message body of the first part of the email. This provided as
        convenience for testing simple messages
    raw_message : str
        The raw message body
    message : email.message
        The email message parsed as an email.message object

    Parameters
    ----------
    message : str
        The raw email body
    """
    def __init__(self, mail_from, recipients, raw_message):
        self.mail_from = mail_from
        self.recipients = recipients
        self.raw_message = raw_message
        self.message = message_from_string(self.raw_message)
        self.headers = dict([(k.lower(), v) for (k, v) in self.message.items()])
        self.body = self._get_body(self.message)

    def _get_body(self, message):
        if message.is_multipart():
            p = message.get_payload()
            if len(p) > 0:
                return p[0].get_payload()
            else:
                return ''
        else:
            return message.get_payload()