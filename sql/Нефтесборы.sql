select pipe_prostoy_uchastok."ID �������� �������", pipe_uchastok_truboprovod."���� �����",
pipe_prostoy_uchastok."���� ��������� ���������",hg.NE_1 "���������", pipe_prostoy_uchastok."L",pipe_uchastok_truboprovod."S"
from 
pipe_prostoy_uchastok LEFT OUTER JOIN class hg on hg.CD_1 = pipe_prostoy_uchastok."���������",
pipe_uchastok_truboprovod
where
pipe_uchastok_truboprovod."���� �����"<=to_date('{date}','YYYY-MM-DD hh24:mi:ss')
and (pipe_prostoy_uchastok."���������" ='HH0004' or pipe_prostoy_uchastok."���� ��������� ���������">to_date('{date}','YYYY-MM-DD hh24:mi:ss'))
and pipe_prostoy_uchastok."ID �������" =pipe_uchastok_truboprovod."ID �������" 
and pipe_uchastok_truboprovod."����������"='HC0002'
{pipe_condition}
{material_condition}
