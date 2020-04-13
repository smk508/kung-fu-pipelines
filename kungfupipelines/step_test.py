from kungfupipelines import step
from kfp import dsl
from caboodle.gcs import get_storage_client
from caboodle.coffer import GCSCoffer
from caboodle.artifacts import PickleArtifact
import os

def test_step():
    
    def step_demo(a, b):
        a = int(a)
        b = int(b)
        return a+2*b

    my_step = step.Step(
        name = 'test',
        function = step_demo,
        arguments = [
            'a',
            'b',
        ],
        fullname = 'step test',
        description = 'in the beginning God created chocolate chip cookies',
    )

    assert my_step(2,3) == 8
    a = 2
    b = 3
    op = my_step.dslContainerOp('image_name', 'ok', a=a, b=b)
    assert type(op) is dsl.ContainerOp
    assert op.arguments == ['ok', 'test', '--a', '2', '--b', '3']

def test_artifact_step():

    # Make input/output Coffer
    a = [1,2,3]
    b = ['a','b','c']
    my_artifacts = [
        PickleArtifact('a.pickle', a),
        PickleArtifact('b.pickle', b)
    ]
    client = get_storage_client()
    input_coffer = GCSCoffer("gs://kung-fu-pipelines-test/artifact-step-inputs", client)
    input_coffer.delete()
    input_coffer.upload(my_artifacts)
    output_coffer = GCSCoffer("gs://kung-fu-pipelines-test/artifact-step-outputs", client)
    output_coffer.delete()
    # Sample function
    downloaded_a = False
    downloaded_b = False
    function_count = 0
    def do_something(filename, output_dir, count, **kwargs):
        nonlocal downloaded_a
        nonlocal downloaded_b
        if filename.endswith("a.pickle"):
            downloaded_a = True
            with open(os.path.join(output_dir, "a_works"), "w") as f:
                f.write("A works.")
        elif filename.endswith("b.pickle"):
            downloaded_b = True
            with open(os.path.join(output_dir, "b_works"), "w") as f:
                f.write("B works.")
        nonlocal function_count
        function_count += count

    my_step = step.ArtifactStep(
        name="testartifacts",
        function=do_something,
        arguments=["function_count"],
        input_coffer=input_coffer, 
        output_coffer=output_coffer,
        )
    
    # Run step
    my_step(1)

    # Check if output was generated.
    assert downloaded_a
    assert downloaded_b
    assert function_count == 2
    
    # Clean up
    input_coffer.delete()
    output_coffer.delete()    
