import abc
import kfp
from kfp import components, dsl, gcp
import wrapt
from typing import Callable, List
from caboodle.cli import Step

def make_sequence(ops: List[dsl.ContainerOp]) -> None:
    """ 
    Makes it so that a sequence of operations in a list take place
    one after another.
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
    
    @abc.abstractmethod
    def compile(self, image:str, script_path:str) -> Callable:
        """ 
        Generates a kfp Pipeline spec which can be used to generate an 
        Argo workflow.yaml
        """
        pass

class BasicWorkflow(Workflow):
    
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
