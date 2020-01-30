from typing import List, Callable
from kfp import dsl
import click
import sys
import pprint
import logging
logger = logging.getLogger(__name__)

def _always_run(*args, **kwargs) -> bool:
    """ This returns False to indicate that the step is not already completed. """
    return False

class Step():
    """
    Represents a step in an Argo/Kubeflow pipeline. This class enables you to provide a function
    along with a set of arguments that the function accepts and perform a variety of tasks related
    to pipeline generation:
    1) Generate a dslContainerOp object which can be used to compile pipeline specification via KFP
    2) Generate a command line tool that can be used to invoke your function. This is helpful 
    because pipeline steps run as some command invoked on a container, usually invoking some
    script that you wrote containing the code to run (see documentation for StepSwitches).
    3) Chain Steps together in a Workflow (see documentation for Workflows).

    Args:
        name: The name of the Step
        function: The function to be called when this Step is invoked
        arguments: A list containing the names of the arguments expected for this function
        fullname: (optional) The fullname of the step
        description: (optional) The description for the step
    """
    
    def __init__(
        self, 
        name:str,
        function: Callable,
        arguments:List[str],
        check_if_complete:Callable = None,
        fullname:str = None,
        description:str = None,
    ):

        self.name = name
        self.function = function
        self.check_if_complete = check_if_complete or _always_run
        self.fullname = fullname or self.name
        self.description = description or ""
        self.arguments = arguments

    def __call__(self, *args, **kwargs):

        if self.check_if_complete(*args, **kwargs):
            self._on_skip(*args, **kwargs)
        else:
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
            arguments = all_arguments,
        )

class GPUStep(Step):
    """
    Represents a Step that requires a GPU to run.
    """
    def dslContainerOp(self, image, script_path, num_gpus=1, **kwargs) -> dsl.ContainerOp:

        op = super().dslContainerOp(image, script_path, **kwargs)
        return op.set_gpu_limit(num_gpus).add_toleration({
            'key': 'nvidia.com/gpu',
            'operator': 'Equal',
            'value': 'present',
            'effect': 'NoSchedule',
        })

class StepSwitch():
    """
    This is essentially a collection of Steps. When called, this reads in 
    command line arguments and runs the appropriate Step along with the provided arguments.
    eg. if you had run python myscript.py step1 a b c --d
    and myscript.py simply creates and calls a StepSwitch, the result would be to call the
    Step named 'step1' with positional arguments a,b,c, and d=True
    """
    def __init__(self, name:str, steps:List[Step]):

        self.name = name,
        self.steps = steps
        self.steps_dict = {step.name: step for step in self.steps}

    def __call__(self):
        """ Parse command line arguments and options to decide which step to run and provide the appropriate arguments. """

        positional, keyword = _parse_cmdline()
        logger.debug("Received arguments %s with keywords %s", positional, keyword)
        step = self.steps_dict[positional[-1]] # Treat the last positional argument as the step name
        logger.debug(step.arguments)
        arguments = {arg.replace('-','_'):keyword[arg] for arg in step.arguments}
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
