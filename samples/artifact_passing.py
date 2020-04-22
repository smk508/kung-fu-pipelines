from kungfupipelines.step import Step, ArtifactStep
from kungfupipelines.cli import StepSwitch
from caboodle.artifacts import BinaryArtifact, PickleArtifact
from caboodle.gcs import get_storage_client
from caboodle.coffer import GCSCoffer
import click
import os

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
    global downloaded_a
    global downloaded_b
    if filename.endswith("a.pickle"):
        downloaded_a = True
        with open(os.path.join(output_dir, "a_works"), "w") as f:
            f.write("A works.")
    elif filename.endswith("b.pickle"):
        downloaded_b = True
        with open(os.path.join(output_dir, "b_works"), "w") as f:
            f.write("B works.")
    global function_count
    function_count += count

step_1 = ArtifactStep(
    name="step1",
    function=function_1,
    arguments=["function_count"],
    input_coffer=input_coffer,
    output_coffer=output_coffer,
    description = "fklda"
    )

workflow = SequentialWorkflow("artifact-passing", [step_1])
workflow.generate_yaml("samples/artifact-passing.yaml", "donka")
switch = StepSwitch("artifact-passing-demo", [step_1])

if __name__=="__main__":
    switch()