select pipe_prostoy_uchastok."ID �������� �������", pipe_uchastok_truboprovod."���� �����"
from 
pipe_prostoy_uchastok,
pipe_uchastok_truboprovod
where
pipe_prostoy_uchastok."ID �������" =pipe_uchastok_truboprovod."ID �������" 
and pipe_prostoy_uchastok."ID �������� �������" IN {pipes}
