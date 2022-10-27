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
        if flow:
            a = x - length
            if a<0:a=0
            b = x + length
            if b>L: b=L
            mask1=(data[:,0]>=a) & (data[:,0]<=b)
        else:
            a = x + length
            if a>L: a=L
            b = x - length
            if b < 0: b = 0
            mask1 = (data[:, 0] >= b) & (data[:, 0] <= a)
        List.append([index[0], a, b])
        data[:,-1][mask1]=True
        mask=data[:,-1]==True

    return np.array(List,dtype=float)

class pipe:
    def __init__(self):
        self.intervals=np.array([],dtype=int)
        self.empty=0
    def append(self,other):
        self.empty=self.empty+other.empty
        self.intervals=np.vstack((self.intervals,other.intervals))