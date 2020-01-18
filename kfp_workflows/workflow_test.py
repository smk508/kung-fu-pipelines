from caboodle import workflow
import inspect

# def test_Pipeline(): # NOTE: This does not work because the introspection doesn't capture optional keyword arguments
# 
#     def f(**kwargs):
#         return kwargs['a'] + 2 * kwargs['b'] + kwargs['c']
# 
#     @workflow.Pipeline(f, name='ok', description='yes')
#     def signature(a, b, c = 7):
#         pass
# 
#     assert signature(a=2, b=3) == 15
#     assert signature(a=2, b=3, c=1) == 9
#     assert inspect.getargspec(signature).args == ['a','b']