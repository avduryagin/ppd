select pipe_prostoy_uchastok."ID простого участка", pipe_uchastok_truboprovod."Дата ввода"
from 
pipe_prostoy_uchastok,
pipe_uchastok_truboprovod
where
pipe_prostoy_uchastok."ID участка" =pipe_uchastok_truboprovod."ID участка" 
and pipe_prostoy_uchastok."ID простого участка" IN {pipes}
