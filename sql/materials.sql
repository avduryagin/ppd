SELECT cl.CD_1,cl.NE_1 FROM class cl WHERE cl.CD_1 IN ( SELECT DISTINCT "Материал трубы" FROM pipe_uchastok_truboprovod)
