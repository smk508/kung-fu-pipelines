from kungfupipelines.step import Step, ArtifactStep
from kungfupipelines.workflow import SequentialWorkflow
from kungfupipelines.cli import StepSwitch
from caboodle.artifacts import BinaryArtifact, PickleArtifact
from caboodle.gcs import get_storage_client
from caboodle.coffer import GCSCoffer
import click
import os

def test_compiler():
    a = [1,2,3]
    b = ['a','b','c']
    my_artifacts = [
        PickleArtifact('a.pickle', a),
        PickleArtifact('b.pickle', b),
    ]
    client = get_storage_client()
    input_coffer = GCSCoffer("gs://kung-fu-pipelines-test/compiler-inputs", client)
    input_coffer.upload(my_artifacts)
    output_coffer = GCSCoffer("gs://kung-fu-pipelines-test/compiler-outputs", client)

    # Sample function
    downloaded_a = False
    downloaded_b = False
    function_count = 0
    def function_1(filename, output_dir, count, **kwargs):
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

    step_1 = ArtifactStep(
        name="step1",
        function=function_1,
        arguments=["function_count"],
        input_coffer=input_coffer,
        output_coffer=output_coffer,
        description = "fklda"
        )

    def hello_world(greeting="Hello", recipient="world", **kwargs):
        click.echo("{0} {1}!".format(greeting, recipient))

    def hello_files(input_filename, output_dir, punctuation="!"):
        with open(input_filename, "rb") as f:
            text = f.read().decode(errors="ignore") + punctuation
        click.echo(text)
        new_filename = input_filename.split("/")[-1]
        with open(os.path.join(output_dir, new_filename), "wb") as f:
            f.write(text.encode("utf-8"))

    step_2 = Step(
        name = "BasicStep",
        function = hello_world,
        arguments = [
            "greeting",
            "recipient",
        ]
    )
    workflow = SequentialWorkflow("artifact-passing", [step_1, step_2])
    workflow.generate_yaml("samples/artifact-passing.yaml", "greeting", "recipient", "function_count")
    switch = StepSwitch("artifact-passing-demo", [step_1])