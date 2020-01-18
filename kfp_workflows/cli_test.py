from caboodle import cli
from kfp import dsl

def test_step():
    
    def step_demo(a, b):
        a = int(a)
        b = int(b)
        return a+2*b

    step = cli.Step(
        name = 'test',
        function = step_demo,
        arguments = [
            'a',
            'b',
        ],
        fullname = 'step test',
        description = 'in the beginning God created chocolate chip cookies',
    )

    assert step(2,3) == 8
    a = 2
    b = 3
    op = step.dslContainerOp('image_name', 'ok', a=a, b=b)
    assert type(op) is dsl.ContainerOp
    assert op.arguments == ['ok', 'test', '--a', '2', '--b', '3']