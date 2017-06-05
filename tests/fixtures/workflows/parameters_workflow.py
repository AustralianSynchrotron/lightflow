from lightflow.models import Parameters, Option, Dag


parameters = Parameters([
    Option('required_arg'),
])


d = Dag('dag')
