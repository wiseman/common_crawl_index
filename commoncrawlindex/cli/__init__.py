import logging
import sys

import gflags


FLAGS = gflags.FLAGS

gflags.DEFINE_enum(
  'logging_level',
  'INFO',
  ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
  'The logging level to use.')


def print_usage(usage=None):
  """Prints executable usage info.

  Prints the script's module-level documentation string as usage info,
  with any embedded '%%s' replaced by sys.argv[0], then prints flag
  info.
  """
  usage_doc = usage or sys.modules['__main__'].__doc__
  if not usage_doc:
    usage_doc = '\nUsage: %s [flags]' % (sys.argv[0],)
  else:
    usage_doc = usage_doc.replace('%s', sys.argv[0])
  usage_doc += '\nFlags:\n%s' % (FLAGS,)
  print usage_doc


class AppError(Exception):
  """An exception that signals an application error.

  When this exception is raised, the exception's message is written to
  stderr.  No stack trace is printed.  A call to sys.exit(1) is then
  used to terminate the application.
  """
  pass


class UsageError(Exception):
  """An exception that signals a command-line usage error by the
  user.

  When this exception is raised, the exception's message is written to
  stderr and then print_usage is called.  No stack trace is printed.
  A call to sys.exit(2) is then used to terminate the application.
  """
  pass


def error(msg, *args):
  """Writes an error message to stderr after flushing stdout."""
  sys.stdout.flush()
  sys.stderr.write('Error: ')
  sys.stderr.write(msg % args)
  sys.stderr.write('\n')


LOGGING_FORMAT = ('%(threadName)s:%(asctime)s:%(levelname)s:%(module)s:'
                  '%(lineno)d %(message)s')


class App(object):
  """The base application object."""
  def __init__(self, main=None, usage=None):
    """
    Args:
      main: The main function.  Defaults to sys.modules['__main__'].name.
    """
    self.main = main or sys.modules['__main__'].main
    assert main
    self.usage = usage

  def configure_logging(self):
    logging.basicConfig(
      level=FLAGS.logging_level,
      format=LOGGING_FORMAT)

  def run(self, argv=None):
    """Runs the main function after parsing command-line flags.

    Also configures logging and sets up profiling if requested with
    --profile.
    """
    if not argv:
      argv = sys.argv
    try:
      FLAGS.UseGnuGetOpt()
      argv = FLAGS(argv)
    except gflags.FlagsError, e:
      error('%s', e)
      print_usage(self.usage)
      sys.exit(2)

    self.configure_logging()
    try:
      self.main(argv)
    except AppError as e:
      error(str(e))
      sys.exit(1)
    except UsageError as e:
      error(str(e))
      print_usage(self.usage)
      sys.exit(2)
