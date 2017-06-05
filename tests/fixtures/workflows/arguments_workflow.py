from lightflow.models import Arguments, Option, Dag


arguments = Arguments([
    Option('required_arg'),
])


d = Dag('dag')
