select pipe_prostoy_uchastok."ID �������� �������", pipe_prostoy_uchastok."L", pipe_uchastok_truboprovod."�������������", hh.NE_1 "�������� �����" from pipe_prostoy_uchastok,pipe_uchastok_truboprovod, class hh 
where  pipe_prostoy_uchastok."���������" ='HH0004' 
and pipe_prostoy_uchastok."ID �������" =pipe_uchastok_truboprovod."ID �������" 
and pipe_uchastok_truboprovod."����������"='HC0002'
and pipe_uchastok_truboprovod."�������� �����"=hh.CD_1