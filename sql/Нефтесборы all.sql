select pipe_prostoy_uchastok."ID �������� �������", pipe_uchastok_truboprovod."���� �����",
pipe_prostoy_uchastok."���� ��������� ���������",hg.NE_1 "���������", pipe_prostoy_uchastok."L",pipe_uchastok_truboprovod."S"
from 
pipe_prostoy_uchastok LEFT OUTER JOIN class hg on hg.CD_1 = pipe_prostoy_uchastok."���������",
pipe_uchastok_truboprovod
where 
pipe_prostoy_uchastok."ID �������" =pipe_uchastok_truboprovod."ID �������" 
and pipe_uchastok_truboprovod."����������"='HC0002'
and pipe_uchastok_truboprovod."��� �������� �����������" in ('HT0007' ,'HT0008' ,'HT0000')
