import numpy as np
import pandas as pd

def get_lambda(lamb=np.array([]),number=0,state=0):
    mask=lamb>=0
    lamb=lamb[mask]
    i=state
    n=1
    N=[]
    N.append(number)
    if number==0:
        return np.array([0])
    while n>0:
        n=np.where(lamb>i)[0].shape[0]
        N.append(n)        #index.append(indices)
        i=i+1
    La = np.array(N, dtype=int)
    Lambda=La[1:La.shape[0]]/La[:La.shape[0]-1]
    return Lambda

def get_vector(c=np.array([]),number=0,state=0):
    mask=c>=0
    tilde=c[mask]
    i=state
    n=1
    N=[]
    index=dict({})
    N.append(number)
    if number==0:
        return index,np.array(N),np.array([0])
    while n>0:
        index.update({i:np.where(c==i)[0]})
        n=len(np.where(tilde>i)[0])
        N.append(n)        #index.append(indices)
        i=i+1
    Nar=np.array(N,dtype=int)
    Ne=Nar[:Nar.shape[0]-1]-Nar[1:Nar.shape[0]]
    return index,Ne

def to_array(function):
    def wrapper(*args,**kwargs):
        result=function(*args,**kwargs)
        try:
            result=np.array(result)
        except AttributeError:
            print('not iterable value')
        return result
    wrapper.__name__=function.__name__
    wrapper.__doc__ = function.__doc__
    return wrapper
@to_array

def get_lambda_decomposition(data,number=1,state=0,stop=5,r=0, hist=np.array([]),top=3,size=10,drift=False):
    epoch = []
    #indices=[]
    #N=[]
    #w=0
    try:
        #per=data[:,r]
        mask=np.where(data[:,r]>=0)[0]
        if mask.shape[0]>0:
            indices, N = get_vector(data[:, r], number=number, state=state)

            for key, n in zip(indices.keys(), N):
                w = r + 1
                idx=np.array([key])
                idx=np.hstack((hist,idx))
                if size>idx.shape[0]:
                    array=np.empty((size-idx.shape[0]))
                    array.fill(-1)
                    array=np.hstack((idx,array))
                else:
                    array=idx[:size]
                hist_=idx
                index=indices[key]

                sub=np.max(data[index,w:w+top],axis=1)
                submask=np.where(sub>=0)[0]
                getout=sub.shape[0]-submask.shape[0]

                if drift:
                    nlamb=n-getout
                else:
                    nlamb=n

                if submask.shape[0]>0:
                    lambd=get_lambda(sub, number=nlamb, state=key)
                    posmask=lambd>0
                    lamb = np.cumprod(lambd[posmask]).sum()
                    shape=idx.shape[0]
                    array=np.hstack((array,nlamb,lamb,shape))
                    epoch.append(array)
                    if r+top<=stop:
                        r_=r+1
                        epoch_= get_lambda_decomposition(data[index], number=n, stop=stop, state=key, r=r_,hist=hist_,top=top,size=size)
                        epoch.extend(epoch_)
            if r == stop:
                return epoch
    except(TypeError):
        print("indices ", indices)
        print("N ", N)
    except IndexError:
        print('index_error',r)
        return epoch
    except(ValueError):
        print("indices ", indices)
        print('w',w,w+r)

    return epoch

def get_joined_matrices(data, time_range=np.array([]), length=100, flow=True, groupby='new_id'):
    groups=data.groupby(groupby)
    pipes=pipe()
    pipes_=pipe()
    for i,group in enumerate(groups):
        enter,L,S=group[1].iloc[0][['Дата ввода','L','S']]
        get_out=group[1]['Дата перевода в бездействие'].max()
        try:
            s,s_=get_parts(group[1][['Наработка до отказа', 'Адрес от начала участка' , 'Дата перевода в бездействие']].values, time_range=time_range, length=length, flow=flow, out=None, L=L, S=S, enter=enter, get_out=get_out)

            if i==0:
                pipes.intervals=s.intervals
                pipes.empty=s.empty
                pipes_.intervals = s_.intervals
                pipes_.empty = s_.empty
            else:
                pipes.append(s)
                pipes_.append(s_)
        except(ValueError):
            print('error in id '+str(group[0]))
    return  pipes,pipes_

def get_time_range(enter=pd.to_datetime('2016-01-01'),get_out=pd.to_datetime('2016-12-31'),S=8,step=1):
    age=(get_out-enter)/np.timedelta64(1,'Y')
    year=pd.to_datetime(enter).year
    first_year=pd.to_datetime(str(year)+'-12-31')
    tau=(first_year-enter)/np.timedelta64(1,'Y')
    #k=int((age-tau+step)/step)
    k=np.ceil((age-tau)/step)
    bound=[]
    for i in np.arange(k+1):
        bound.append(tau+step*i)
    return np.array(bound)/S

def get_tr(top=40, step=0.5,S=8):
    if step==0:
        return None
    count=np.ceil(top/step)
    bounds=[]
    size=step
    while count>0:
        bounds.append(size)
        size=size+step
        count=count-1
    return np.array(bounds)/S

def get_parts(data, time_range=np.array([]), step=1, length=100., flow=True, **args):
    List_minus = []
    List_plus = []
    if data.shape[0] == 0: return np.array(List_plus), np.array(List_minus)
    L = args['L']
    S = args['S']
    enter = args['enter']
    get_out = args['get_out']

    if time_range.shape[0] == 0:
        time_range = get_time_range(enter, get_out, S, step=step)
    tau = time_range[0]
    delta=0
    if time_range.shape[0]>=1:
        delta = time_range[1]-tau

    intervals = split(data[:, 1].reshape(-1, 1), L=L, length=length, flow=flow)
    for i in np.arange(intervals.shape[0]):
        index, a, b = intervals[i, :]
        index=int(index)
        r=data[index][0]/S
        x = data[index][1]
        if delta==0:
            current=0
        else:
            current=np.ceil((r-tau)/delta)
        s_plus = []
        s_minus = []
        if flow:
            mask_min=(data[:,1]>=x)&(data[:,1]<=b)
            mask_plus=(data[:,1]>=a)&(data[:,1]<=x)
            s_minus.append(x)
            s_minus.append(b)
            s_plus.append(a)
            s_plus.append(x)
        else:
            mask_min=(data[:,1]>=b)&(data[:,1]<=x)
            mask_plus=(data[:,1]>=x)&(data[:,1]<=a)
            s_minus.append(b)
            s_minus.append(x)
            s_plus.append(x)
            s_plus.append(a)
        minus_sub = data[mask_min]
        plus_sub = data[mask_plus]
        minus_out=minus_sub[:,2].max()
        plus_out = plus_sub[:, 2].max()
        mlife = ((minus_out - enter) / np.timedelta64(1, 'Y')) / S
        plife = ((plus_out - enter) / np.timedelta64(1, 'Y')) / S

        mout = 0
        pout = 0

        if delta > 0:
            mout = np.ceil((mlife - tau) / delta)
            pout=np.ceil((plife - tau) / delta)

        for j, t in enumerate(time_range):
            if j < current:
                s_plus.append(0)
                s_minus.append(0)
            else:
                if j>mout:
                    s_minus.append(-1)
                else:
                    min_mask = np.where(minus_sub[:, 0] / S <= time_range[j])[0]
                    s_minus.append(min_mask.shape[0])

                if j>pout:
                    s_plus.append(-1)
                else:
                    plus_mask = np.where(plus_sub[:, 0] / S <= time_range[j])[0]
                    s_plus.append(plus_mask.shape[0])

        List_plus.append(s_plus)
        List_minus.append(s_minus)

    empty=np.ceil(L/length)
    minus=pipe()
    minus.empty=empty
    minus.intervals=np.array(List_minus, dtype=int)
    plus=pipe()
    plus.empty = empty
    plus.intervals=np.array(List_plus, dtype=int)
    #return np.array(List_plus, dtype=int), np.array(List_minus, dtype=int)
    return plus,minus

def split(data=np.array([]), L=100, length=100., flow=True):

    List = []
    data=np.hstack((data,np.zeros(shape=(data.shape[0],1))))
    mask1 = (data[:,0] > L) | (data[:,0] < 0)
    data=data[~mask1]
    indices = np.arange(data.shape[0], dtype=int)
    mask=data[:,-1]==True
    while not all(mask):
        index = indices[~mask]
        x = data[index][0, 0]


        a = x - length
        if a<0:a=0
        b = x + length
        if b>L: b=L
        abound=a
        bbound=b

        mask1=(data[:,0]>=abound) & (data[:,0]<=bbound)
        List.append([index[0], abound, bbound])
        data[:,-1][mask1]=True
        mask=data[:,-1]==True

    return np.array(List,dtype=float)

def get_cell(x, time_range=np.array([])):
    i=0
    while i<time_range.shape[0]:
        val=time_range[i]
        if val<=x:
            i+=1
            continue
        else:
            return i
        i+=1
    return time_range.shape[0]-1

class pipe:
    def __init__(self):
        self.intervals=np.array([],dtype=int)
        self.empty=0
        self.cell=0
    def append(self,other):
        self.empty=self.empty+other.empty
        self.cell=max(self.cell,other.cell)
        self.intervals=np.vstack((self.intervals,other.intervals))

class Decomposition:
    def __init__(self,ncell=30,step=1.125):
        assert (type(ncell)==int) and ncell>0,"ncell must be a positive integer"
        assert (type(step) == float) and step > 0, "step must be a positive float"
        self.ncell=ncell
        self.step=step
        self.free_cells=np.zeros(ncell)
        self.leave_cells = np.zeros(ncell)
        self.time_range=self.get_range(self.ncell,self.step)
    def get_range(self,ncell,step):
        l=np.empty(ncell)
        l[0]=step
        i=1
        while i<ncell:
            l[i]=l[i-1]+step
            i+=1
        return l


    def split(self,data=np.array([]), L=100, length=100., flow=True):
        def borders(List, x=0,L=100,length=100):
            def get_span(List,x):
                amax = np.NINF
                bmin = np.inf
                for l in List:
                    a = l[2]
                    b = l[1]
                    if x < b:
                        if b < bmin:
                            bmin = b
                    if x > a:
                        if a > amax:
                            amax = a
                return amax,bmin
            a_,b_=get_span(List,x)
            dea=x-a_
            deb=b_-x
            ahat=min(x,dea)
            bhat=min(deb,L-x)
            if (ahat<length)&(bhat<length):
                return ahat,bhat
            da=max(length-ahat,0)
            db=max(length-bhat,0)
            abo=min(length+db,ahat)
            bbo=min(length+da,bhat)
            return abo,bbo



        List = []
        #data = np.hstack((data, np.zeros(shape=(data.shape[0], 1))))
        mask1 = (data[:, 0] > L) | (data[:, 0] < 0)
        data = data[~mask1]
        indices = np.arange(data.shape[0], dtype=int)
        mask=np.zeros(data.shape[0],dtype=bool)
        #mask = data[:, -1] == True
        while not all(mask):
            index = indices[~mask]
            x = data[index][0, 0]
            a_, b_ = borders(List, x,L,length)
            abound=x-a_
            bbound=x+b_
            mask1 = (data[:, 0] >= abound) & (data[:, 0] <= bbound)
            List.append([index[0], abound, bbound])
            mask[mask1]=True
            #data[:, -1][mask1] = True
            #mask = data[:, -1] == True

        return np.array(List, dtype=float)

    def matrix(self, data=np.array([]), labels=np.array([]),time_range=np.array([]), length=100., flow=True, **args):
        trajectories = []
        if data.shape[0] == 0: return None
        L = args['L']
        S = args['S']
        tau = time_range[0]
        intervals = self.split(data[:, 1].reshape(-1, 1), L=L, length=length, flow=flow)
        for i in intervals:
            index, a, b = i
            index=int(index)
            r = data[index][0] / S
            x = data[index][1]
            cell=self.get_cell(r,self.time_range)
            label=labels[index]
            trajectories.append([label,cell,a,b])
        return np.array(trajectories)

    def get_cell(self,x, time_range=np.array([])):
        i = 0
        #max_i=i
        while i < time_range.shape[0]:
            val = time_range[i]
            if val <= x:
                #max_i=i
                i += 1
                continue
            else:
                return i
            i += 1
        return time_range.shape[0] - 1

    def get_joined(self,data=pd.DataFrame([]), time_range=np.array([]), length=100, flow=True, groupby='new_id'):
        groups = data.groupby(groupby)
        pipes = dict()

        for i, group in enumerate(groups):
            enter, L, S = group[1].iloc[0][['Дата ввода', 'L', 'S']]
            ID=group[0]
            get_out = group[1]['Дата перевода в бездействие'].max()
            age=(get_out-enter)/np.timedelta64(1,'Y')
            rage=age/S
            frame=group[1][['Наработка до отказа', 'Адрес от начала участка', 'Дата перевода в бездействие']]
            try:
                s = self.matrix(frame.values, labels=frame.index,time_range=time_range, length=length, flow=flow,
                                        out=None, L=L, S=S, enter=enter, get_out=get_out)
                pipe_=pipe()
                pipe_.intervals=s
                pipe_.empty=self.cell_number(L,length)
                pipe_.cell=self.get_cell(rage,time_range)
                pipes[ID]=pipe_

            except(ValueError):
                print('error in id ' + str(group[0]))
        return pipes
    def cells_di(self,pipe_=np.array([],dtype=np.int32),ncell=1,cell=0):
        i=0
        count=0
        while i<pipe_.shape[0]:
            n=pipe_[i]
            if n>cell:
                i+=1
                continue
            if n<cell:
                ncell-=1
            if cell==n:
                count+=1
            i+=1
        return count,ncell

    def cell_count(self,pipe=pipe(),cell=0):
        if pipe.cell>=cell:
            if pipe.intervals.shape[0]==0:
                return 0,pipe.empty
            count,ncell=self.cells_di(pipe.intervals[:,1],pipe.empty,cell+1)
            return count,ncell
        else:
            return 0,0



    def dcells(self,pipe_=np.array([],dtype=np.int32),ncell=1,cell=0,iter_=0):
        i=0
        count=0
        top=cell+iter_
        while i<pipe_.shape[0]:
            n=pipe_[i]
            if n>top:
                i+=1
                continue
            if n<cell:
                ncell-=1
            if (cell<=n)&(n<=top):
                #print(n)
                count+=1
            i+=1
        return count,ncell
    def cell_number(self,Length,length):
        ncells=np.ceil(Length/(2*length))
        return ncells

    def fill(self,x=np.array([]),y=np.array([]),size=100,a=-0.125/2,b=0.125/2):
        lamb = []
        i=0
        while i<x.shape[0]:
            la=y[i]
            index=x[i]
            x_ = np.zeros(size)
            y_ = np.zeros(size)
            y_.fill(la)
            x_.fill(index)
            dx = np.random.random(x_.shape[0]) * (b-a)+a
            x_ = x_ + dx
            lamb.append([x_, y_])
            i+=1
        return np.hstack(lamb).T


class Lambda_Decomposition:
    def __init__(self,ncell=30,step=1.125):
        assert (type(ncell)==int) and ncell>0,"ncell must be a positive integer"
        assert (type(step) == float) and step > 0, "step must be a positive float"
        self.ncell=ncell
        self.step=step
        self.free_cells=np.zeros(ncell)
        self.leave_cells = np.zeros(ncell)
        self.time_range=self.get_range(self.ncell,self.step)
    def get_range(self,ncell,step):
        l=np.empty(ncell)
        l[0]=step
        i=1
        while i<ncell:
            l[i]=l[i-1]+step
            i+=1
        return l


    def split(self,data=np.array([]), L=100, length=100., flow=True):
        def borders(List, x=0,L=100,length=100):
            def get_span(List,x):
                amax = np.NINF
                bmin = np.inf
                for l in List:
                    a = l[2]
                    b = l[1]
                    if x < b:
                        if b < bmin:
                            bmin = b
                    if x > a:
                        if a > amax:
                            amax = a
                return amax,bmin
            a_,b_=get_span(List,x)
            dea=x-a_
            deb=b_-x
            ahat=min(x,dea)
            bhat=min(deb,L-x)
            if (ahat<length)&(bhat<length):
                return ahat,bhat
            da=max(length-ahat,0)
            db=max(length-bhat,0)
            abo=min(length+db,ahat)
            bbo=min(length+da,bhat)
            return abo,bbo



        List = []
        #data = np.hstack((data, np.zeros(shape=(data.shape[0], 1))))
        mask1 = (data[:, 0] > L) | (data[:, 0] < 0)
        data = data[~mask1]
        indices = np.arange(data.shape[0], dtype=int)
        mask=np.zeros(data.shape[0],dtype=bool)
        #mask = data[:, -1] == True
        while not all(mask):
            index = indices[~mask]
            x = data[index][0, 0]
            a_, b_ = borders(List, x,L,length)
            abound=x-a_
            bbound=x+b_

            #a = x - length
            #if a < 0: a = 0
            #b = x + length
            #if b > L: b = L
            #abound = max(a_, a)
            #bbound = min(b_, b)
            #print(x,a,b,a_,b_,abound,bbound,bbound-abound)
            mask1 = (data[:, 0] >= abound) & (data[:, 0] <= bbound)
            List.append([index[0], abound, bbound])
            mask[mask1]=True
            #data[:, -1][mask1] = True
            #mask = data[:, -1] == True

        return np.array(List, dtype=float)

    def matrix(self, data=np.array([]), labels=np.array([]),time_range=np.array([]), length=100., flow=True, **args):
        trajectories = []
        if data.shape[0] == 0: return None
        L = args['L']
        S = args['S']
        tau = time_range[0]
        intervals = self.split(data[:, 1].reshape(-1, 1), L=L, length=length, flow=flow)
        for i in intervals:
            index, a, b = i
            index=int(index)
            r = data[index][0] / S
            x = data[index][1]
            #cell=self.get_cell(r,self.time_range)
            label=labels[index]
            trajectories.append([label,r,a,b])
        return np.array(trajectories)

    def get_cell(self,x, time_range=np.array([])):
        i = 0
        #max_i=i
        while i < time_range.shape[0]:
            val = time_range[i]
            if val <= x:
                #max_i=i
                i += 1
                continue
            else:
                return i
            i += 1
        return time_range.shape[0] - 1

    def get_joined(self,data=pd.DataFrame([]), time_range=np.array([]), length=100, flow=True, groupby='new_id'):
        groups = data.groupby(groupby)
        pipes = dict()

        for i, group in enumerate(groups):
            enter, L, S = group[1].iloc[0][['Дата ввода', 'L', 'S']]
            ID=group[0]
            get_out = group[1]['Дата перевода в бездействие'].max()
            age=(get_out-enter)/np.timedelta64(1,'Y')
            rage=age/S
            frame=group[1][['Наработка до отказа', 'Адрес от начала участка', 'Дата перевода в бездействие']]
            assert (age>0)&(S>0)&(get_out>enter)&(L>0),"incorrect data. ID {0} ".format(ID)
            #print("ID {0}".format(ID))
            try:
                s = self.matrix(frame.values, labels=frame.index,time_range=time_range, length=length, flow=flow,
                                        out=None, L=L, S=S, enter=enter, get_out=get_out)
                pipe_=pipe()
                pipe_.intervals=s
                pipe_.empty=self.cell_number(L,length)
                pipe_.cell=rage
                pipes[ID]=pipe_

            except(ValueError):
                print('error in id ' + str(group[0]))
        return pipes
    def cells_di(self,pipe_=pipe(),r=0.):
        i=0
        dr=self.step
        ncell=pipe_.empty
        if pipe_.cell<r:
            return 0,0
        count=0
        while i<pipe_.intervals.shape[0]:
            ri=pipe_.intervals[i,1]
            if ri>r+dr:
                i+=1
                continue
            if ri<r:
                ncell-=1
                i+=1
                continue
            if (r<=ri)&(ri<=r+dr):
                count+=1
            i+=1
        return count,ncell

    def masked_cll(self,pipe_=pipe(),mask=None,r=0.):

        dr=self.step
        ncell=pipe_.empty
        if pipe_.cell<r:
            return 0,0
        count=0
        if mask is None:
            array=pipe_.intervals
        else:
            assert mask.shape[0]==pipe_.intervals.shape[0],"Mask shape must be equal an intervals shape"
            assert mask.dtype==bool, "Dtype the mask must be boolean"
            array=pipe_.intervals[mask]
        i = 0
        while i<array.shape[0]:
            ri=array[i,1]
            if ri>r+dr:
                i+=1
                continue
            if ri<r:
                ncell-=1
                i+=1
                continue
            if (r<=ri)&(ri<=r+dr):
                count+=1
            i+=1
        return count,ncell

    def cell_count(self,pipe=pipe(),r=0):
        dr=self.step
        if pipe.cell>=r:
            if pipe.intervals.shape[0]==0:
                return 0,pipe.empty
            count,ncell=self.cells_di(pipe,r,dr)
            return count,ncell
        else:
            return 0,0



    def dcells(self,pipe_=np.array([],dtype=np.int32),ncell=1,cell=0,iter_=0):
        i=0
        count=0
        top=cell+iter_
        while i<pipe_.shape[0]:
            n=pipe_[i]
            if n>top:
                i+=1
                continue
            if n<cell:
                ncell-=1
            if (cell<=n)&(n<=top):
                #print(n)
                count+=1
            i+=1
        return count,ncell
    def cell_number(self,Length,length):
        ncells=np.ceil(Length/(2*length))
        return ncells

    def fill(self,x=np.array([]),y=np.array([]),size=100,a=-0.125/2,b=0.125/2):
        lamb = []
        i=0
        while i<x.shape[0]:
            la=y[i]
            index=x[i]
            x_ = np.zeros(size)
            y_ = np.zeros(size)
            y_.fill(la)
            x_.fill(index)
            dx = np.random.random(x_.shape[0]) * (b-a)+a
            x_ = x_ + dx
            lamb.append([x_, y_])
            i+=1
        return np.hstack(lamb).T

