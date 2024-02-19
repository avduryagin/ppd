select pipe_prostoy_uchastok."ID простого участка", pipe_uchastok_truboprovod."Дата ввода",
pipe_prostoy_uchastok."Дата изменения состояния",hg.NE_1 "Состояние", pipe_prostoy_uchastok."L",pipe_uchastok_truboprovod."S"
from 
pipe_prostoy_uchastok LEFT OUTER JOIN class hg on hg.CD_1 = pipe_prostoy_uchastok."Состояние",
pipe_uchastok_truboprovod
where 
pipe_prostoy_uchastok."ID участка" =pipe_uchastok_truboprovod."ID участка" 
and pipe_uchastok_truboprovod."Назначение"='HC0002'
and pipe_uchastok_truboprovod."Вид покрытия внутреннего" in ('HT0007' ,'HT0008' ,'HT0000')
