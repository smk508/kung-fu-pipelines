from kungfupipelines.step import Step, ArtifactStep
from kungfupipelines.cli import StepSwitch
from caboodle.artifacts import BinaryArtifact
from caboodle.gcs import get_storage_client
from caboodle.coffer import GCSCoffer
import click
import os

def hello_world(greeting="Hello", recipient="world", **kwargs):
    click.echo("{0} {1}!".format(greeting, recipient))

def hello_files(input_filename, output_dir, punctuation="!"):
    with open(input_filename, "rb") as f:
        text = f.read().decode(errors="ignore") + punctuation
    click.echo(text)
    new_filename = input_filename.split("/")[-1]
    with open(os.path.join(output_dir, new_filename), "wb") as f:
        f.write(text.encode("utf-8"))

sample_step = Step(
    name = "BasicStep",
    function = hello_world,
    arguments = [
        "greeting",
        "recipient",
    ]
)

a = "howdy there".encode()
b = "ayy wassup".encode()

my_artifacts = [
    BinaryArtifact('a.txt', content=a),
    BinaryArtifact('b.txt', content=b),
]
client = get_storage_client()
input_coffer = GCSCoffer("gs://kung-fu-pipelines-test/switch-step-inputs", client)
input_coffer.upload(my_artifacts)
output_coffer = GCSCoffer("gs://kung-fu-pipelines-test/switch-step-outputs", client)

sample_artifact_step = ArtifactStep(
    "ArtifactStep",
    function = hello_files,
    arguments = [
        "punctuation"
    ],
    input_coffer=input_coffer,
    output_coffer=output_coffer,
)

switch = StepSwitch(
    "step_demo",
    steps = [
        sample_step,
        sample_artifact_step,
    ],
)

if __name__=="__main__":
    switch()