select pipe_prostoy_uchastok."ID простого участка", pipe_prostoy_uchastok."L", pipe_uchastok_truboprovod."Месторождение", hh.NE_1 "Материал трубы" from pipe_prostoy_uchastok,pipe_uchastok_truboprovod, class hh 
where  pipe_prostoy_uchastok."Состояние" ='HH0004' 
and pipe_prostoy_uchastok."ID участка" =pipe_uchastok_truboprovod."ID участка" 
and pipe_uchastok_truboprovod."Назначение"='HC0002'
and pipe_uchastok_truboprovod."Материал трубы"=hh.CD_1