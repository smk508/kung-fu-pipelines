import pprint
import sys
from kungfupipelines.step import Step
from typing import List
import logging

logger = logging.getLogger(__name__)

class StepSwitch():
    """
    This is essentially a collection of Steps. When called, this reads in 
    command line arguments and runs the appropriate Step along with the provided arguments.
    eg. if you had run python myscript.py step1 a b c --d
    and myscript.py simply creates and calls a StepSwitch, the result would be to call the
    Step named 'step1' with positional arguments a,b,c, and d=True
    """
    def __init__(self, name:str, steps:List[Step]):

        self.name = name
        self.steps = steps
        self.steps_dict = {step.name: step for step in self.steps}

    def __call__(self):
        """ Parse command line arguments and options to decide which step to run and provide the appropriate arguments. """

        positionals, keywords = _parse_cmdline()
        logger.debug("Received arguments %s with keywords %s", positionals, keywords)
        if len(positionals) == 0:
            print("{0} \n------\nOptions:\n {1}".format(
                self.name,
                "\n ".join("{0} - {1}".format(s.name, s.description) for s in self.steps),
                )
            )
            return
        step = self.steps_dict[positionals[-1]] # Treat the last positional argument as the step name
        logger.debug(step.arguments)
        arguments = {arg.replace('-','_'):keywords[arg] for arg in step.arguments if arg in keywords}
        logger.info("Running step '%s' with arguments %s", step.fullname, pprint.pformat(arguments))
        return step(**arguments)

def _parse_cmdline():
    """
    Parses command line arguments for use by a StepSwitch.
    Flag arguments are converted to True (eg. command --x would produce the argument x=True)
    """
    pos = []
    named = {}
    key = None
    args = sys.argv[1:]
    for arg in args:
        if key:
            if arg.startswith('--'):
                named[key] = True # Indicate a positional flag
                key = arg[2:]#.replace('-','_')
            else:
                named[key] = arg
                key = None
        elif arg.startswith('--'):
            key = arg[2:]#.replace('-','_')
        else:
            pos.append(arg)
    if key:
        named[key] = True
    return (pos, named)
