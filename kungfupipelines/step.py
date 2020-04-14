from caboodle.coffer import Coffer, LocalCoffer
from caboodle.artifacts import Artifact
from typing import List, Dict, Callable
from kfp import dsl
import pprint
import logging
import os
import shutil
from tqdm import tqdm

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
        arguments: List[str],
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

    def _on_skip(self, *args, **kwargs):
        """ This is ran if the step is skipped. """
        logger.info("Skipping step {0} because it has already been completed.".format(self.fullname))

    def dslContainerOp(self, image, command=None, **kwargs) -> dsl.ContainerOp:
        """
        Returns a dsl.ContainerOp that runs the Step function.
        The command is going to be something like:
            image command name arguments
        "command" refers to the path to the binary/script. This script could
        represent a StepSwitch, in which case the first argument to it has to be
        the name of the Step you want to run. Following that, we insert the
        arguments to the Step.
        """
        positionals = []
        if command:
            positionals.append(command)
        positionals.append(self.name) 
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

class ArtifactStep(Step):
    """
    Represents a Step which does the following three things:
    1) Download input artifacts from remote storage.
    2) Perform operations on artifacts.
    3) Upload output artifacts to remote storage.

    1 and 3 are automatically handled here, so you only have to implement logic
    for 2 (along with specifying the bucket+folder where to download/upload your
    artifacts).

    This is a common use-case, as you may have some software which you just want
    to run against files, and you need this as a step in your workflow.
    """

    def __init__(
        self,
        name:str,
        function: Callable,
        arguments:List[str],
        input_coffer: Coffer,
        output_coffer: Coffer,
        check_if_complete:Callable = None,
        fullname:str = None,
        description:str = None,
        local_input="/tmp/input/",
        local_output="/tmp/output/",
    ):
        super().__init__(name, function, arguments, check_if_complete, fullname, description)
        self.input_coffer = input_coffer
        self.output_coffer = output_coffer
        self.local_input = local_input
        self.local_output = local_output

    def __call__(self, *args, **kwargs):

        if self.check_if_complete(*args, **kwargs):
            self._on_skip(*args, **kwargs)
        else:
            return self.download_run_upload(*args, **kwargs)

    def download_run_upload(self, *args, **kwargs) -> List[Artifact]:
        """
        Downloads artifacts from input_coffer, runs the step's function on each
        artifact, and uploads results to output_coffer.
        It is assumed that the function takes as its first argument a filepath
        (location on disk of artifact) and as second argument a directory path
        (folder where to send results)
        # TODO: Enable storing artifacts in memory.
        """
        # Download
        logger.info("Beginning {0} step. Downloading artifacts from {1}".format(self.name, self.input_coffer.location))
        self.input_coffer.download(self.local_input)
        
        # Compute
        for f in tqdm([x for x in os.listdir(self.local_input)]):
            filename = os.path.join(self.local_input, f)
            self.function(filename, self.local_output, *args, **kwargs)

        # Upload
        logger.info("{0} step completed. Now Uploading artifacts to {1}".format(self.name, self.output_coffer.location))
        self.output_coffer.upload_folder(self.local_output)

    # def download_run_upload(*args, **kwargs):
    # 
    #     # Download artifacts
    #     logger.info("Downloading files from {0}.".format(self.input_coffer.location))
    #     artifacts = self.input_coffer.download()
    #     # Compute
    #     output = self.function(artifacts, *args, **kwargs)
    #     # Upload results
    #     logger.info("Uploading files to {0}".format(self.output_coffer.location))
    #     self.output_coffer.upload(output)

# class BinaryExecutableStep(ArtifactStep):
#     """
#     This is an ArtifactStep where the operation is a binary executable which is
#     run against each of the files in the input, and the output files are
#     to be uploaded.
#     """
#     def __init__(
#         self,
#         *args,
#         **kwargs,
#     ):
#         super().__init__(self, *args, **kwargs)

