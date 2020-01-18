from typing import List, Callable
from kfp import dsl
import click
import sys
import pprint
import logging
logger = logging.getLogger(__name__)

class Step():
    """
    Represents a step in an Argo workflow.
    """
    
    def __init__(
        self, 
        name:str,
        function: Callable,
        arguments:List[str],
        fullname:str = None,
        description:str = None,
    ):

        self.name = name
        self.function = function
        self.fullname = fullname or self.name
        self.description = description or ""
        self.arguments = arguments

    def __call__(self, *args, **kwargs):

        return self.function(*args, **kwargs)

    def dslContainerOp(self, image, script_path, **kwargs) -> dsl.ContainerOp:
        """
        Returns a dsl.ContainerOp that runs the Step function.
        """
        positionals = [script_path, self.name]
        options = []
        for arg in self.arguments:
            if arg in kwargs:
                options += ['--{0}'.format(arg), kwargs[arg]]
        all_arguments = positionals + options
        return dsl.ContainerOp(
            name = self.fullname,
            image = image,
            arguments = all_arguments
        )
    
class StepSwitch():

    def __init__(self, name:str, steps:List[Step]):

        self.name = name,
        self.steps = steps
        self.steps_dict = {step.name: step for step in self.steps}

    def __call__(self):
        """ Parse command line arguments and options to decide which step to run and provide the appropriate arguments. """

        positional, keyword = parse_cmdline()
        logger.debug("Received arguments %s with keywords %s", positional, keyword)
        step = self.steps_dict[positional[-1]] # Treat the last positional argument as the step name
        logger.debug(step.arguments)
        arguments = {arg.replace('-','_'):keyword[arg] for arg in step.arguments}
        logger.info("Running step '%s' with arguments %s", step.fullname, pprint.pformat(arguments))
        return step(**arguments)

def parse_cmdline():
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
