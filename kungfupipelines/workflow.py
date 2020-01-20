import abc
import kfp
from kfp import components, dsl, gcp
import wrapt
from typing import Callable, List
from kungfupipelines.cli import Step

def make_sequence(ops: List[dsl.ContainerOp]) -> None:
    """ 
    Links a sequence of pipeline operations so that they are configured
    to take place one after another.
    Args:
        ops - list[dsl.ContainerOp]
    """
    l = len(ops)
    if l <= 1:
        return
    i = 0
    before = ops[i]
    for op in ops[1:]:
        op.after(before)
        before = op
    
class Workflow(abc.ABC):
    """
    A Workflow abstracts the connectivity structure between pipeline steps. Individual steps
    can be provided to a Workflow, and they will then be compiled together so that they happen
    in a predefined sequence specified by that Workflow. This makes is easy to have 'standardized'
    pipeline structures in which you can simply swap out individual steps as needed.
    For example, a machine learning workflow might have slots for dataset preprocessing, training,
    hyperparameter optimization, etc. You can provide specific pipeline steps to fill in those slots
    and generate the pipeline spec without having to rewrite the connectivity structure each time.
    """
    @abc.abstractmethod
    def compile(self, image:str, script_path:str) -> Callable:
        """ 
        Generates a kfp Pipeline spec which can be used to generate an 
        Argo workflow.yaml
        Args:
            image: The uri for the image to use.
            script_path: The path that your script is located in in the image.
        """
        pass

class BasicMLWorkflow(Workflow):
    """
    This specifies a simple pipeline for machiine learning. It consists of the following steps 
    taking place one after another:
    1) create/download/acquire the master dataset
    2) perform a train/test split
    3) apply any preprocessing logic
    4) train your model
    5) apply any post processing logic using the test set, including computing accuracy, ROC, etc.

    Args:
        name: Name to use for the compiled pipeline
        image: Docker container URI containing all of the scripts
        script_path: Path to script that runs the steps
        make_dataset: The Step to use for making the dataset
        train_test_split: The Step to use for train_test_split
        preprocess: The Step to use for preprocessing
        train: The Step to use for model training
        postprocess_ops: A list of Steps to perform post-training operations with
        description: An optional string describing your pipeline
    """
    def __init__(
        self,
        name: str,
        image: str,
        script_path: str,
        make_dataset: Step,
        train_test_split: Step,
        preprocess: Step,
        train: Step,
        postprocess_ops: List[Step] = [],
        description: str = None,
    ):
        self.make_dataset = make_dataset
        self.train_test_split = train_test_split
        self.preprocess = preprocess
        self.train = train
        self.postprocess_ops = postprocess_ops
        self.image = image
        self.script_path = script_path

    def compile(self):
        
        def pipeline(*args, **kwargs):

            make_dataset_op = self.make_dataset.dslContainerOp(self.image, self.script_path, **kwargs)
            train_test_split_op = self.train_test_split.dslContainerOp(self.image, self.script_path, **kwargs)
            train_op = self.train.dslContainerOp(
                self.image, self.script_path, **kwargs
                ).set_gpu_limit(1).add_toleration({
                    'key': 'nvidia.com/gpu',
                    'operator': 'Equal',
                    'value': 'present',
                    'effect': 'NoSchedule'
                    })
            postprocess_ops = [
                pp.dslContainerOp(self.image, self.script_path, **kwargs)
                for pp in self.postprocess_ops
            ]

            make_sequence([make_dataset_op, train_test_split_op, train_op])
            for pp in postprocess_ops:
                pp.after(train_op)

        return pipeline

def Pipeline(pipeline_func: Callable, name:str, description:str=''): # NOTE: This does not work

    @dsl.pipeline(name=name, description=description)
    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        return pipeline_func(*args, **kwargs)
    
    return wrapper 

def generate_yaml(self, filename):
    """
    Generates an argo workflow.yaml spec which can be used to submit this
    workflow to Argo / Kubeflow.
    """
    pipeline = self.compile()
    kfp.compiler.Compile().compile(pipeline, filename)
