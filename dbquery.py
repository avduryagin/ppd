import cx_Oracle
import pandas as pd
import numpy as np
import os
import warnings

warnings.filterwarnings('ignore')



class session:
    def __init__(self,host='192.168.0.97',service_name='ORA11',port='1521',user='PIPE_NNG',password='PIPE_NNG',**kwargs):
        self.host=host
        self.service_name=service_name
        self.port=port
        self.user=user
        self.log=''
        self.password=password
        self.dsn=cx_Oracle.makedsn(host=self.host,port=self.port,service_name=self.service_name,**kwargs)
        self.connection=None
        self.connection_mn = None
        self.lib_dir=r"C:\oracle\instantclient_19_21"
        self.path=os.path.join(os.path.dirname(os.path.abspath(__file__)),'sql')

        #self.path = os.path.join(os.getcwd(), 'sql')
        if not os.path.isdir(self.path):
            os.mkdir(self.path)
        try:
            cx_Oracle.init_oracle_client(lib_dir=self.lib_dir)
        except Exception as error:
            pass

    def open(self):
        self.connection = cx_Oracle.connect(user=self.user, password=self.password, dsn=self.dsn, encoding="UTF-8")
        self.connection_mn = cx_Oracle.connect(user='PIPE_NNG_MN', password='PIPE_NNG_MN', dsn=self.dsn, encoding="UTF-8")

    def wrap(self,x):
        if len(x) > 1:
            return x
        elif len(x)==1:
            return """('""" + str(x[0]) + """')"""
        else:
            return """('"""  """')"""
    def get_empty_pipes(self,date=np.datetime64("2014-01-01"),except_list=tuple(),sql_file = 'Нефтесборы.sql'):

        with open(os.path.join(self.path, sql_file), 'r') as f:
            sql = f.read()
        assert type(except_list)==tuple,'set of IDs must be a tuple'
        pipe_condition = ""
        if len(except_list)>0:
            pipe_condition = 'pipe_prostoy_uchastok."ID простого участка" not in {pipes} and'.format(pipes=self.wrap(except_list))
        sql = sql.format(date=date, pipe_condition=pipe_condition)
        nng=pd.read_sql(sql,self.connection)
        nng_mn=pd.read_sql(sql,self.connection_mn)
        data=pd.concat([nng,nng_mn],axis=0,ignore_index=True)
        del nng
        del nng_mn
        return data
    def get_all_empty(self,sql_file = 'Нефтесборы all.sql'):

        with open(os.path.join(self.path, sql_file), 'r') as f:
            sql = f.read()

        nng=pd.read_sql(sql,self.connection)
        nng_mn=pd.read_sql(sql,self.connection_mn)
        data=pd.concat([nng,nng_mn],axis=0,ignore_index=True)
        del nng
        del nng_mn
        return data

    def get_accidents(self,date=np.datetime64("2014-01-01"),sql_file = 'Отказы общий.sql'):

        with open(os.path.join(self.path, sql_file), 'r') as f:
            sql = f.read()
        nng=pd.read_sql(sql,self.connection)
        nng_mn=pd.read_sql(sql,self.connection_mn)
        data=pd.concat([nng,nng_mn],axis=0,ignore_index=True)
        del nng
        del nng_mn
        return data

class session_gen(session):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
    def open(self):
        self.connection = cx_Oracle.connect(user=self.user, password=self.password,
                                            dsn=self.dsn, encoding="UTF-8")
    def get_empty_pipes(self,date=np.datetime64("2014-01-01"),inner_surface=tuple(['HT0007' ,'HT0008' ,'HT0000']),materials=tuple(['HB0000']),sql_file = 'Нефтесборы.sql'):

        with open(os.path.join(self.path, sql_file), 'r') as f:
            sql = f.read()

        assert type(inner_surface)==tuple,'set of IDs must be a tuple'
        pipe_condition = ""
        if len(inner_surface)>0:
            pipe_condition = 'and pipe_uchastok_truboprovod."Вид покрытия внутреннего" in {surface}'.format(surface=self.wrap(inner_surface))
        material_condition=""
        if len(materials)>0:
            material_condition = 'and pipe_uchastok_truboprovod."Материал трубы" in {materials}'.format(materials=self.wrap(materials))
        sql = sql.format(date=date, pipe_condition=pipe_condition,material_condition=material_condition)

        data=pd.read_sql(sql,self.connection)
        return data
    def get_all_empty(self,sql_file = 'Нефтесборы all.sql'):
        with open(os.path.join(self.path, sql_file), 'r') as f:
            sql = f.read()
        data=pd.read_sql(sql,self.connection)
        return data

    def get_accidents(self,inner_surface=tuple(['HT0007' ,'HT0008' ,'HT0000']),materials=tuple(['HB0000']),sql_file = 'Отказы общий.sql'):
        with open(os.path.join(self.path, sql_file), 'r') as f:
            sql = f.read()
        assert type(inner_surface)==tuple,'set of IDs must be a tuple'
        pipe_condition = ""
        if len(inner_surface)>0:
            pipe_condition = 'and pipe_uchastok_truboprovod."Вид покрытия внутреннего" in {surface}'.format(surface=self.wrap(inner_surface))
        material_condition=""
        if len(materials)>0:
            material_condition = 'and pipe_uchastok_truboprovod."Материал трубы" in {materials}'.format(materials=self.wrap(materials))
        sql = sql.format(pipe_condition=pipe_condition,material_condition=material_condition)

        data=pd.read_sql(sql,self.connection)
        return data
    def waterpercent_values(self,ID=0,sql_file='wregime.sql'):
        with open(os.path.join(self.path, sql_file), 'r') as f:
            sql = f.read()
        sql = sql.format(ID=ID)
        data = pd.read_sql(sql, self.connection)
        return data


class generalized_session:
    def __init__(self,con_parameters=None):
        if con_parameters is None:
            con_parameters={'nng':{'host':'192.168.0.97','service_name':'ORA11',
                                   'port':'1521','user':'PIPE_NNG','password':'PIPE_NNG'}}
        self.con_parameters=con_parameters
        self.session=None

    def open(self):
        self.session=dict()
        for k in self.con_parameters.keys():
            args=self.con_parameters[k]
            host = args['host']
            service_name = args['service_name']
            port = args['port']
            user = args['user']
            password = args['password']
            session=session_gen(host=host,service_name=service_name,port=port,user=user,password=password)
            session.open()
            self.session[k]=session

    def get_empty_pipes(self,date,*args,sql_params=dict(),**kwargs):
        if self.session is None:
            self.open()
        data_list=[]
        for sid in self.session.keys():
            session=self.session[sid]
            try:
                kwargs_=sql_params[sid]
            except KeyError:
                kwargs_=kwargs
                print('scheme {0} not found'.format(sid) )
            data=session.get_empty_pipes(date,*args,**kwargs_)
            data_list.append(data)
        data = pd.concat(data_list, axis=0, ignore_index=True)
        return data

    def get_accidents(self,*args,sql_params=dict(),**kwargs):
        if self.session is None:
            self.open()
        data_list=[]
        for sid in self.session.keys():
            session=self.session[sid]
            try:
                kwargs_=sql_params[sid]
            except KeyError:
                kwargs_=kwargs
            data=session.get_accidents(*args,**kwargs_)
            data_list.append(data)
        data = pd.concat(data_list, axis=0, ignore_index=True)
        return data
    def get_all_empty(self,*args,**kwargs):
        if self.session is None:
            self.open()
        data_list=[]
        for sid in self.session.keys():
            session=self.session[sid]
            data=session.get_all_empty(*args,**kwargs)
            data_list.append(data)
        data = pd.concat(data_list, axis=0, ignore_index=True)
        return data
    def waterpercent(self,ID=0,sql_file='wregime.sql'):
        if self.session is None:
            self.open()
        data_list=[]
        for sid in self.session.keys():
            session=self.session[sid]
            data=session.waterpercent_values(ID=ID,sql_file=sql_file)
            data_list.append(data)
        data = pd.concat(data_list, axis=0, ignore_index=True)
        data=data.sort_values(by='Дата расчета')
        return data

    def read_sql(self,sql_file=''):
        if self.session is None:
            self.open()
        data_list=[]
        for sid in self.session.keys():
            session=self.session[sid]
            data = pd.read_sql(sql_file, session.connection)
            data_list.append(data)
        data = pd.concat(data_list, axis=0, ignore_index=True)
        return data

























