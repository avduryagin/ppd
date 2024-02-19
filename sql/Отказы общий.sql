select 
PIPE_UCHASTOK_TRUBOPROVOD."Вид покрытия внутреннего",
PIPE_PROSTOY_UCHASTOK."Начало простого участка"||'  ->   '||PIPE_PROSTOY_UCHASTOK."Конец простого участка" "Название",
pipe_avaria."ID простого участка",
to_number(substr("Месторождение",3,4))*5 "Месторождение",
D,
pipe_prostoy_uchastok.L,
S,
HB.NE_1 "Материал трубы",
HO.NE_1 "Тип трубы",
HU.NE_1 "Завод изготовитель",
to_char("Дата ввода",'yyyy-mm-dd') "Дата ввода",
HH.NE_1 "Состояние",
decode(pipe_prostoy_uchastok."Состояние", 'HH0004',null,TO_char(pipe_prostoy_uchastok."Дата изменения состояния",'yyyy-mm-dd')) "Дата перевода в бездействие",
TO_char("Дата аварии",'yyyy-mm-dd') "Дата аварии",
("Дата аварии" - "Дата ввода")/365.25 "Наработка до отказа",
PIPE_AVARIA."Адрес от начала участка" "Адрес от начала участка",
nvl( PIPE_AVARIA."Обводненность", SELECT_REGIM('Обводненность',pipe_avaria."ID простого участка" , to_char("Дата аварии" ,'dd.mm.yyyy') ) ) "Обводненность",
PIPE_AVARIA."Скорость потока",
nvl2(to_char(pipe_avaria."ID ремонта"), 
(select HM3.NE_1 from  pipe_remont pr, class HM3
where HM3.CD_1(+)=pr."Тип ремонта"  
 and pr."ID ремонта" =pipe_avaria."ID ремонта")
, HM.NE_1) "Способ ликв в момент",
pipe_remont."ID ремонта",
to_char(pipe_remont."Дата окончания ремонта",'yyyy-mm-dd') "Дата окончания ремонта",
pipe_rem.Rem_adres_pu(pipe_remont."ID ремонта",
                      PIPE_AVARIA."ID простого участка",3  ) "Адрес рем от начала участка",
pipe_rem.Rem_adres_pu(pipe_remont."ID ремонта",
                      PIPE_AVARIA."ID простого участка",2  )"Длина ремонтируемого участка",
HM2.NE_1 "Тип ремонта",
pr_do."ID ремонта" "ID ремонта до аварии",
TO_char(pr_do."Дата окончания ремонта",'yyyy-mm-dd') "Дата ремонта до аварии",
pipe_rem.Rem_adres_pu(pr_do."ID ремонта",
                      PIPE_AVARIA."ID простого участка",3  )"Адрес ремонта до аварии",
pipe_rem.Rem_adres_pu(pr_do."ID ремонта",
                      PIPE_AVARIA."ID простого участка",2  )"Длина ремонта до аварии",
HM4.NE_1 "Тип ремонта до аварии"
from 
pipe_avaria
LEFT OUTER JOIN pipe_remont
                   LEFT OUTER JOIN class HM2 on HM2.CD_1=pipe_remont."Тип ремонта"
 on
  pipe_remont."ID ремонта"=pipe_rem.Obr_Av_Rem_zam(PIPE_AVARIA."ID аварии", 
                                                   PIPE_AVARIA."Дата аварии",
                                                   PIPE_AVARIA."ID простого участка", 
                                                   PIPE_AVARIA."Адрес от начала участка")
LEFT OUTER JOIN class HM on HM.CD_1 = PIPE_AVARIA."Способ ликвидации СНГ"
LEFT OUTER JOIN pipe_remont pr_do 
            LEFT OUTER JOIN class HM4 on HM4.CD_1=pr_do."Тип ремонта"
  on   
   pr_do."ID ремонта"=pipe_rem.Obr_Av_Rem_zam_do(pipe_avaria."ID аварии", 
                                                 pipe_avaria."Дата аварии",
                                                 PIPE_AVARIA."ID простого участка", 
                                                 PIPE_AVARIA."Адрес от начала участка")
,
pipe_prostoy_uchastok
LEFT OUTER JOIN class HH on HH.CD_1 = pipe_prostoy_uchastok."Состояние"
,
pipe_uchastok_truboprovod
LEFT OUTER JOIN class HB on HB.CD_1 = "Материал трубы"
LEFT OUTER JOIN class HO on HO.CD_1 = "Тип трубы"
LEFT OUTER JOIN class HU on HU.CD_1 = "Завод изготовитель"
where 
 pipe_avaria."ID простого участка" = pipe_prostoy_uchastok."ID простого участка"
and pipe_prostoy_uchastok."ID участка"=pipe_uchastok_truboprovod."ID участка"
and pipe_uchastok_truboprovod."Назначение"='HC0002'
and "Обьект аварии" in ('FE0001')
/*AND pipe_uchastok_truboprovod."S" <> NULL
AND pipe_uchastok_truboprovod."L" <> NULL
AND "Дата ввода" <> NULL
AND "Дата аварии" <> NULL*/
AND "Дата ввода" IS NOT NULL
AND "Дата аварии" IS NOT NULL
AND pipe_uchastok_truboprovod."S" IS NOT NULL
AND pipe_uchastok_truboprovod."S" >0
AND pipe_prostoy_uchastok."L" IS NOT NULL
AND pipe_prostoy_uchastok."L" >0
AND "Дата аварии">="Дата ввода"
{pipe_condition}
{material_condition}
order by pipe_avaria."ID простого участка","Дата аварии"
