SELECT* from(
select PIPE_TEHNOLOG_REGIM."���� �������" ,PIPE_TEHNOLOG_REGIM."�������������"
FROM PIPE_TEHNOLOG_REGIM
WHERE PIPE_TEHNOLOG_REGIM."ID �������� �������"={ID}
UNION ALL
select PIPE_TEHNOLOG_REGIM_OLD."���� �������" ,PIPE_TEHNOLOG_REGIM_OLD."�������������"
FROM PIPE_TEHNOLOG_REGIM_OLD
WHERE PIPE_TEHNOLOG_REGIM_OLD."ID �������� �������"={ID})