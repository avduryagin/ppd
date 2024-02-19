select 
PIPE_UCHASTOK_TRUBOPROVOD."��� �������� �����������",
PIPE_PROSTOY_UCHASTOK."������ �������� �������"||'  ->   '||PIPE_PROSTOY_UCHASTOK."����� �������� �������" "��������",
pipe_avaria."ID �������� �������",
to_number(substr("�������������",3,4))*5 "�������������",
D,
pipe_prostoy_uchastok.L,
S,
HB.NE_1 "�������� �����",
HO.NE_1 "��� �����",
HU.NE_1 "����� ������������",
to_char("���� �����",'yyyy-mm-dd') "���� �����",
HH.NE_1 "���������",
decode(pipe_prostoy_uchastok."���������", 'HH0004',null,TO_char(pipe_prostoy_uchastok."���� ��������� ���������",'yyyy-mm-dd')) "���� �������� � �����������",
TO_char("���� ������",'yyyy-mm-dd') "���� ������",
("���� ������" - "���� �����")/365.25 "��������� �� ������",
PIPE_AVARIA."����� �� ������ �������" "����� �� ������ �������",
nvl( PIPE_AVARIA."�������������", SELECT_REGIM('�������������',pipe_avaria."ID �������� �������" , to_char("���� ������" ,'dd.mm.yyyy') ) ) "�������������",
PIPE_AVARIA."�������� ������",
nvl2(to_char(pipe_avaria."ID �������"), 
(select HM3.NE_1 from  pipe_remont pr, class HM3
where HM3.CD_1(+)=pr."��� �������"  
 and pr."ID �������" =pipe_avaria."ID �������")
, HM.NE_1) "������ ���� � ������",
pipe_remont."ID �������",
to_char(pipe_remont."���� ��������� �������",'yyyy-mm-dd') "���� ��������� �������",
pipe_rem.Rem_adres_pu(pipe_remont."ID �������",
                      PIPE_AVARIA."ID �������� �������",3  ) "����� ��� �� ������ �������",
pipe_rem.Rem_adres_pu(pipe_remont."ID �������",
                      PIPE_AVARIA."ID �������� �������",2  )"����� �������������� �������",
HM2.NE_1 "��� �������",
pr_do."ID �������" "ID ������� �� ������",
TO_char(pr_do."���� ��������� �������",'yyyy-mm-dd') "���� ������� �� ������",
pipe_rem.Rem_adres_pu(pr_do."ID �������",
                      PIPE_AVARIA."ID �������� �������",3  )"����� ������� �� ������",
pipe_rem.Rem_adres_pu(pr_do."ID �������",
                      PIPE_AVARIA."ID �������� �������",2  )"����� ������� �� ������",
HM4.NE_1 "��� ������� �� ������"
from 
pipe_avaria
LEFT OUTER JOIN pipe_remont
                   LEFT OUTER JOIN class HM2 on HM2.CD_1=pipe_remont."��� �������"
 on
  pipe_remont."ID �������"=pipe_rem.Obr_Av_Rem_zam(PIPE_AVARIA."ID ������", 
                                                   PIPE_AVARIA."���� ������",
                                                   PIPE_AVARIA."ID �������� �������", 
                                                   PIPE_AVARIA."����� �� ������ �������")
LEFT OUTER JOIN class HM on HM.CD_1 = PIPE_AVARIA."������ ���������� ���"
LEFT OUTER JOIN pipe_remont pr_do 
            LEFT OUTER JOIN class HM4 on HM4.CD_1=pr_do."��� �������"
  on   
   pr_do."ID �������"=pipe_rem.Obr_Av_Rem_zam_do(pipe_avaria."ID ������", 
                                                 pipe_avaria."���� ������",
                                                 PIPE_AVARIA."ID �������� �������", 
                                                 PIPE_AVARIA."����� �� ������ �������")
,
pipe_prostoy_uchastok
LEFT OUTER JOIN class HH on HH.CD_1 = pipe_prostoy_uchastok."���������"
,
pipe_uchastok_truboprovod
LEFT OUTER JOIN class HB on HB.CD_1 = "�������� �����"
LEFT OUTER JOIN class HO on HO.CD_1 = "��� �����"
LEFT OUTER JOIN class HU on HU.CD_1 = "����� ������������"
where 
 pipe_avaria."ID �������� �������" = pipe_prostoy_uchastok."ID �������� �������"
and pipe_prostoy_uchastok."ID �������"=pipe_uchastok_truboprovod."ID �������"
and pipe_uchastok_truboprovod."����������"='HC0002'
and "������ ������" in ('FE0001')
/*AND pipe_uchastok_truboprovod."S" <> NULL
AND pipe_uchastok_truboprovod."L" <> NULL
AND "���� �����" <> NULL
AND "���� ������" <> NULL*/
AND "���� �����" IS NOT NULL
AND "���� ������" IS NOT NULL
AND pipe_uchastok_truboprovod."S" IS NOT NULL
AND pipe_uchastok_truboprovod."S" >0
AND pipe_prostoy_uchastok."L" IS NOT NULL
AND pipe_prostoy_uchastok."L" >0
AND "���� ������">="���� �����"
{pipe_condition}
{material_condition}
order by pipe_avaria."ID �������� �������","���� ������"
