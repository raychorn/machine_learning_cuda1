def binner(list_of_events):
    import numpy as np
    import pandas as pd
    
    df_d = pd.DataFrame(list_of_events)

    df_d = df_d.groupby(["dstport"],  as_index=False).sum()
    
    #BPP
    df_d["bpp"] = df_d["bytes"]/df_d["packets"] 
    df_d["bpp_norm"] =  np.log(df_d['bpp'])
    df_d["bpp_zscore+log"] = (df_d["bpp_norm"] - df_d["bpp_norm"].mean()) / df_d["bpp_norm"].std()   
    
    return df_d.values.tolist()
