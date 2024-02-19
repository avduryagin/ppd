import numpy as np

def random_cell(labels=np.array([]),ncell=0):
    if ncell==0:
        return np.array([])
    index=np.arange(labels.shape[0])
    ulabels,ucounts=np.unique(labels,return_counts=True)
    uprobab=ucounts/labels.shape[0]
    kcell= {u:0 for u in ulabels}
    probab={u:p for u,p in zip(ulabels,uprobab)}
    mask=np.ones(index.shape[0],dtype=bool)
    index_=index.copy()
    i=0
    while i<ncell:
        shuffled=np.random.permutation(index_)
        k=shuffled[0]
        label=labels[k]
        kcell[label]+=1
        mask[k]=False
        index_=index[mask]
        i+=1
    return kcell,probab


