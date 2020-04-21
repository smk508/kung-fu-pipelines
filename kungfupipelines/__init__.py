from .step import Step, ArtifactStep
from .cli import StepSwitch
from .workflow import *

from kfp.compiler import Compiler

import json
from collections import defaultdict
from deprecated import deprecated
import inspect
import tarfile
import uuid
import zipfile
from typing import Callable, Set, List, Text, Dict, Tuple, Any, Union, Optional

class KungFuCompiler(Compiler):
    """
    Overrides some of the methods in the standard compiler to automate aspects
    of pipeline construction when using Steps.
    """

    def _create_workflow(self,
        pipeline_func: Callable,
        pipeline_name: Text=None,
        pipeline_description: Text=None,
        params_list: List[dsl.PipelineParam]=None,
        pipeline_conf: dsl.PipelineConf=None,
        steps: List[Step]=None,
        ) -> Dict[Text, Any]:
        """ 
        Creates a workflow while automatically loading in parameters from Step
        arguments. 
        """
        params_list = params_list or []
        # argspec = inspect.getfullargspec(pipeline_func)
        all_args = [step.arguments for step in steps]
        args = [x for y in all_args for x in y]

        # Create the arg list with no default values and call pipeline function.
        # Assign type information to the PipelineParam
        pipeline_meta = _extract_pipeline_metadata(pipeline_func)
        pipeline_meta.name = pipeline_name or pipeline_meta.name
        pipeline_meta.description = pipeline_description or pipeline_meta.description
        pipeline_name = sanitize_k8s_name(pipeline_meta.name)

        # Need to first clear the default value of dsl.PipelineParams. Otherwise, it
        # will be resolved immediately in place when being to each component.
        default_param_values = {}
        for param in params_list:
        default_param_values[param.name] = param.value
        param.value = None

        # Currently only allow specifying pipeline params at one place.
        if params_list and pipeline_meta.inputs:
        raise ValueError('Either specify pipeline params in the pipeline function, or in "params_list", but not both.')


        args_list = []
        for arg_name in args: #argspec.args:
            arg_type = None
        for input in pipeline_meta.inputs or []:
            if arg_name == input.name:
            arg_type = input.type
            break
        args_list.append(dsl.PipelineParam(sanitize_k8s_name(arg_name, True), param_type=arg_type))

        with dsl.Pipeline(pipeline_name) as dsl_pipeline:
        pipeline_func(*args_list)

        pipeline_conf = pipeline_conf or dsl_pipeline.conf # Configuration passed to the compiler is overriding. Unfortunately, it's not trivial to detect whether the dsl_pipeline.conf was ever modified.

        self._validate_exit_handler(dsl_pipeline)
        self._sanitize_and_inject_artifact(dsl_pipeline, pipeline_conf)

        # Fill in the default values.
        args_list_with_defaults = []
        if pipeline_meta.inputs:
        args_list_with_defaults = [dsl.PipelineParam(sanitize_k8s_name(arg_name, True))
                                    for arg_name in argspec.args]
        # if argspec.defaults:
        #     for arg, default in zip(reversed(args_list_with_defaults), reversed(argspec.defaults)):
        #     arg.value = default.value if isinstance(default, dsl.PipelineParam) else default
        elif params_list:
        # Or, if args are provided by params_list, fill in pipeline_meta.
        for param in params_list:
            param.value = default_param_values[param.name]

        args_list_with_defaults = params_list
        pipeline_meta.inputs = [
            InputSpec(
                name=param.name,
                type=param.param_type,
                default=param.value) for param in params_list]

        op_transformers = [add_pod_env]
        op_transformers.extend(pipeline_conf.op_transformers)

        workflow = self._create_pipeline_workflow(
            args_list_with_defaults,
            dsl_pipeline,
            op_transformers,
            pipeline_conf,
        )

        from kfp.compiler._data_passing_rewriter import fix_big_data_passing
        workflow = fix_big_data_passing(workflow)

        import json
        workflow.setdefault('metadata', {}).setdefault('annotations', {})['pipelines.kubeflow.org/pipeline_spec'] = json.dumps(pipeline_meta.to_dict(), sort_keys=True)

        return workflow
