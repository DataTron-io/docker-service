import json
import logging
import pickle
import pandas as pd
from pandas import DataFrame
import pandas as pd
from sklearn.externals import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import sklearn
import warnings
from numpy import nan
import datetime
from datetime import date
import time
from random import randint
import random
import traceback



class ModelPredictor(object):
    """
    This class is modified by the user to upload the model into the Datatron platform.
    """
    def __init__(self):
        pass

    def predict(self, x):
        """
        Required for online and offline predictions

        :param: x : A list or list of list of input vector
        :return: single prediction

        Example (Copy and paste the 3 lines to test it out):
        Step 1. Load the model into python. Model artifacts are stored in "models" folder
        model = pickle.load(open("models/xgboost_birth_model.pkl", "rb"))

        Step 2. Prepare the data to be predicted. This needs to be modified based on how the data was sent in the
        request
        x= pd.DataFrame(x, columns = self.feature_list())

        Step 3. Use the uploaded model to predict/ infer from the input data
        return model.predict(x)

        Note: Make sure all the needed packages are mentioned in requirements.txt
        """



        # In[ ]:

        MODEL_ID = 'EBI_N19_08_008'
        MODEL_PREFIX = "PRO_AC_PML"


        # In[ ]:

        def datamap(data_full):

            for x in data_full.columns:
              try:
                data_full[x] = data_full[x].astype(float)
              except:
                continue

            num_df = data_full[data_full.select_dtypes(include=['float64','int64']).columns].fillna(999999)
            cat_df = data_full[data_full.select_dtypes(include=['O']).columns].replace(' ','NA').fillna('NA').replace('null','NA').apply(lambda x: x.astype(str).str.upper())
            fin_df = pd.concat([num_df, cat_df],axis=1)

            return fin_df

        # In[ ]:

        def datascore_m(data, varb, obj, score_name, pred_name):
            print ('data',data)
            pred=obj.predict(data[varb['Var_name']])[0]
            print ("pred",pred)
            data[pred_name] = obj.predict(data[varb['Var_name']])[0]
            print ("----predict:",obj.predict(data[varb['Var_name']])[0])
            data['s1'] = obj.predict_proba(data[varb['Var_name']])[:,0].round(10)
            data['s2'] = obj.predict_proba(data[varb['Var_name']])[:,1].round(10)
            data['s3'] = obj.predict_proba(data[varb['Var_name']])[:,1].round(10)
            data[score_name] = data[['s1','s2','s3']].max(axis=1)
            data.drop(['s1','s2','s3'],inplace=True,axis=1)
            #display (data)
            return data


        # In[ ]:

        def datascore_b(data, varb, obj, score_name, pred_name):

            data[pred_name] = obj.predict(data[varb['Var_name']])
            data[score_name] = obj.predict_proba(data[varb['Var_name']])[:,1].round(10)

            return data

        # In[ ]:

        def tier_adjust(f, req_tc1_pct, req_tc2_pct, tiers_score, tiers_pred, tier_chng1, tier_chng2, tier_extra, new_tier):

            random.seed(1234)

            f_extra = f[f[tiers_pred]==tier_extra]

            # current prediction values
            curr1 = f[f[tiers_pred]==tier_chng1].shape[0]
            curr2 = f[f[tiers_pred]==tier_chng2].shape[0]
            #print(curr1,curr2)

            # required prediction values
            req1 = req_tc1_pct*f.shape[0]
            req2 = req_tc2_pct*f.shape[0]
            #print(req1,req2)

            # gap between required and current predictions
            diff1 = req1 - curr1 
            diff2 = req2 - curr2
            #print(round(diff1),round(diff2))

            if diff1 < 0:

                # randomly sample diff from tier1 to tier3
                e1 = f[f[tiers_pred]==tier_chng1]
                #print(abs(round(diff1*randint(97,99)/100)))
                e1_0 = e1.sample(n=abs(round(diff1*randint(98,100)/100)))
                e1_1 = e1[~(e1.index.isin(e1_0.index))]
                e1_1[new_tier] = tier_chng1
                e1_0[new_tier] = tier_chng2

                # randomly sample diff from tier2 to tier3
                req_ex_pct = 1 - req_tc1_pct-req_tc2_pct

                diff_extra = f_extra.shape[0]-req_ex_pct*f.shape[0]
                fe_0 = f_extra.sample(n=round(abs(diff_extra)*randint(98,100)/100))
                fe_1 = f_extra[~(f_extra.index.isin(fe_0.index))]
                fe_1[new_tier] = tier_extra
                fe_0[new_tier] = tier_chng2 

                e2 = f[f[tiers_pred]==tier_chng2]
                e2[new_tier] = tier_chng2

                r = e1_1.append(e1_0)
                r = r.append(fe_1)
                r = r.append(fe_0)
                r = r.append(e2)

            else:

                f_chng = f[~(f[tiers_pred]==tier_extra)]
                #print(f_extra.shape, f_chng.shape)

                tier1 = f_extra[f_extra[tiers_score]>f_extra[tiers_score].median()]
                tier0 = f_extra[f_extra[tiers_score]<=f_extra[tiers_score].median()]
                t0_cnt = tier0.shape[0]
                #print(tier1.shape, t0_cnt)

                # new required percentage from extra preds in tier 1
                new_t1_pct = 0.01; #diff1/t0_cnt *randint(97,99)/100
                new_t2_pct = 0.01; # diff2/(t0_cnt-diff1) *randint(97,99)/100
                #print(new_t1_pct,new_t2_pct)

                tier1[new_tier] = tier_extra # everyone above median of original prediction
                f_chng.loc[f_chng[tiers_pred]==tier_chng1,new_tier] = tier_chng1
                f_chng.loc[f_chng[tiers_pred]==tier_chng2,new_tier] = tier_chng2

                # assigning updated tiers
                # randomly selecting rows from tier1 prediction
                try:

                    tier0_1 = tier0.sample(frac=new_t1_pct)
                    tier0_rem = tier0[~(tier0.index.isin(tier0_1.index))]
                    tier0_2 = tier0_rem.sample(frac=new_t2_pct)
                    tier0_rem = tier0_rem[~(tier0_rem.index.isin(tier0_2.index))]

                except:

                    print("Using 60th percentile")
                    tier1 = f_extra[f_extra[tiers_score]>np.percentile(f_extra[tiers_score],60)]
                    tier0 = f_extra[f_extra[tiers_score]<=np.percentile(f_extra[tiers_score],60)]
                    t0_cnt = tier0.shape[0]

                    # new required percentage from extra preds in tier 1
                    new_t1_pct = diff1/t0_cnt *randint(97,99)/100
                    new_t2_pct = diff2/(t0_cnt-diff1) *randint(97,99)/100
                    #print(new_t1_pct,new_t2_pct)

                    tier1[new_tier] = tier_extra # everyone above median of original prediction
                    f_chng.loc[f_chng[tiers_pred]==tier_chng1,new_tier] = tier_chng1
                    f_chng.loc[f_chng[tiers_pred]==tier_chng2,new_tier] = tier_chng2

                    tier0_1 = tier0.sample(frac=new_t1_pct)
                    tier0_rem = tier0[~(tier0.index.isin(tier0_1.index))]
                    tier0_2 = tier0_rem.sample(frac=new_t2_pct)
                    tier0_rem = tier0_rem[~(tier0_rem.index.isin(tier0_2.index))]


                tier0_rem[new_tier] = tier_extra
                tier0_1[new_tier] = tier_chng1
                tier0_2[new_tier] = tier_chng2

                r = f_chng.append(tier1)
                r = r.append(tier0_1)
                r = r.append(tier0_2)
                r = r.append(tier0_rem)

                #print(r[new_tier].value_counts())

            return r   

        # In[ ]:

        def tier_adjust_2(f, req_tc1_pct, tiers_score, tiers_pred, tier_chng1, tier_extra, new_tier):

            random.seed(12345)

            f_extra = f[f[tiers_pred]==tier_extra]
            f_chng = f[~(f[tiers_pred]==tier_extra)]

            # current prediction values
            curr1 = f[f[tiers_pred]==tier_chng1].shape[0]

            # required prediction values
            req1 = req_tc1_pct*f.shape[0]

            # gap between required and current predictions
            diff1 = req1 - curr1 

            tier1 = f_extra[f_extra[tiers_score]>f_extra[tiers_score].median()]
            tier0 = f_extra[f_extra[tiers_score]<=f_extra[tiers_score].median()]
            t0_cnt = tier0.shape[0]

            # new required percentage from extra preds in tier 1
            new_t1_pct = diff1/t0_cnt *randint(97,99)/100

            tier1[new_tier] = tier_extra # everyone above median of original prediction
            f_chng.loc[f_chng[tiers_pred]==tier_chng1,new_tier] = tier_chng1

            # assigning updated tiers
            # randomly selecting rows from tier1 prediction
            tier0_1 = tier0.sample(frac=new_t1_pct)
            tier0_rem = tier0[~(tier0.index.isin(tier0_1.index))]

            tier0_rem[new_tier] = tier_extra
            tier0_1[new_tier] = tier_chng1

            r = f_chng.append(tier1)
            r = r.append(tier0_1)
            r = r.append(tier0_rem)

            return r 

        # In[ ]:

        def tier_adjust_3(f,req_tc1_pct,req_tc2_pct,req_tc3_pct,tiers_score,tiers_pred,tier_chng1,tier_chng2,tier_chng3,tier_extra,new_tier):

            random.seed(897)

            print(f[tiers_pred].value_counts())
            f_extra = f[f[tiers_pred]==tier_extra]
            f_chng = f[~(f[tiers_pred]==tier_extra)]
            print(f_extra.shape[0],f_chng.shape[0])

            # current prediction values
            curr1 = f[f[tiers_pred]==tier_chng1].shape[0]
            curr2 = f[f[tiers_pred]==tier_chng2].shape[0]
            curr3 = f[f[tiers_pred]==tier_chng3].shape[0]

            # required prediction values
            req1 = req_tc1_pct*f.shape[0]
            req2 = req_tc2_pct*f.shape[0]
            req3 = req_tc3_pct*f.shape[0]

            # gap between required and current predictions
            diff1 = req1 - curr1 
            diff2 = req2 - curr2
            diff3 = req3 - curr3


            try:
                tier1 = f_extra[f_extra[tiers_score]>np.percentile(f_extra[tiers_score],60)]
                #tier0 = f_extra[f_extra[tiers_score]<=f_extra[tiers_score].median()]
                tier0 = f_extra[~(f_extra.index.isin(tier1.index))]
                t0_cnt = tier0.shape[0]
                print(f_extra.shape[0],tier1.shape[0],t0_cnt)
                print("6p :",np.percentile(f_extra[tiers_score],60))

                # new required percentage from extra preds in tier 1
                new_t1_pct = diff1/t0_cnt *randint(99,100)/100
                new_t2_pct = diff2/(t0_cnt-diff1) *randint(97,99)/100
                new_t3_pct = diff3/(t0_cnt-diff1-diff2) *randint(97,99)/100
                print(diff1/t0_cnt,diff2/(t0_cnt-diff1),diff3/(t0_cnt-diff1-diff2))
                #print(new_t1_pct,new_t2_pct,new_t3_pct)

                tier1[new_tier] = tier_extra # everyone above median of original prediction
                f_chng.loc[f_chng[tiers_pred]==tier_chng1,new_tier] = tier_chng1
                f_chng.loc[f_chng[tiers_pred]==tier_chng2,new_tier] = tier_chng2
                f_chng.loc[f_chng[tiers_pred]==tier_chng3,new_tier] = tier_chng3

                # assigning updated tiers
                # randomly selecting rows from tier1 prediction
                tier0_1 = tier0.sample(frac=new_t1_pct)
                tier0_rem = tier0[~(tier0.index.isin(tier0_1.index))]
                tier0_2 = tier0_rem.sample(frac=new_t2_pct)
                tier0_rem = tier0_rem[~(tier0_rem.index.isin(tier0_2.index))]
                tier0_3 = tier0_rem.sample(frac=new_t3_pct)
                tier0_rem = tier0_rem[~(tier0_rem.index.isin(tier0_3.index))]

                tier0_rem[new_tier] = tier_extra
                tier0_1[new_tier] = tier_chng1
                tier0_2[new_tier] = tier_chng2
                tier0_3[new_tier] = tier_chng3

                r = f_chng.append(tier1)
                r = r.append(tier0_1)
                r = r.append(tier0_2)
                r = r.append(tier0_3)
                r = r.append(tier0_rem)

            except:

                tier1 = f_extra[f_extra[tiers_score]>f_extra[tiers_score].median()]
                #tier0 = f_extra[f_extra[tiers_score]<=np.percentile(f_extra[tiers_score],60)]
                tier0 = f_extra[~(f_extra.index.isin(tier1.index))]
                t0_cnt = tier0.shape[0]
                print("med :", f_extra[tiers_score].median())
                print(f_extra.shape[0],tier1.shape[0],t0_cnt)

                req_ext_pct = 1-req_tc1_pct-req_tc2_pct-req_tc3_pct
                req_ext = f_extra.shape[0]* req_ext_pct
                diff_ext = f_extra.shape[0]- req_ext

                if t0_cnt < abs(diff_ext): 
                    print("less than criteria")
                    rmv = tier1.shape[0] - req_ext
                    tier1 = tier1.sort_values(by=tiers_score, ascending=True)
                    tier1_0 = tier0.head(round(rmv))
                    tier1_0[new_tier] = tier_chng1 # less probability values in > median bucket
                    tier1_rem = tier1[~(tier1.index.isin(tier1_0.index))]
                    tier1_rem[new_tier] = tier_extra # desired preds of higher probability
                    diff1_n = diff1 - tier1_0.shape[0]

                    # new required percentage from extra preds in tier 1
                    new_t1_pct = (diff1_n)/t0_cnt *randint(99,100)/100
                    new_t2_pct = diff2/(t0_cnt-diff1_n) *randint(97,99)/100
                    new_t3_pct = diff3/(t0_cnt-diff1_n-diff2) *randint(97,99)/100
                    print(diff1_n/t0_cnt,diff2/(t0_cnt-diff1_n),diff3/(t0_cnt-diff1_n-diff2))
                    print(new_t1_pct,new_t2_pct,new_t3_pct)

                    f_chng.loc[f_chng[tiers_pred]==tier_chng1,new_tier] = tier_chng1
                    f_chng.loc[f_chng[tiers_pred]==tier_chng2,new_tier] = tier_chng2
                    f_chng.loc[f_chng[tiers_pred]==tier_chng3,new_tier] = tier_chng3

                    tier0_1 = tier0.sample(frac=new_t1_pct)
                    tier0_rem = tier0[~(tier0.index.isin(tier0_1.index))]
                    tier0_2 = tier0_rem.sample(frac=new_t2_pct)
                    tier0_rem = tier0_rem[~(tier0_rem.index.isin(tier0_2.index))]
                    tier0_3 = tier0_rem.sample(frac=new_t3_pct)
                    tier0_rem = tier0_rem[~(tier0_rem.index.isin(tier0_3.index))]

                    tier0_1[new_tier] = tier_chng1
                    tier0_2[new_tier] = tier_chng2
                    tier0_3[new_tier] = tier_chng3

                    r = f_chng.append(tier1_rem)
                    r = r.append(tier1_0)
                    r = r.append(tier0_1)
                    r = r.append(tier0_2)
                    r = r.append(tier0_3)        


                else:
                    # new required percentage from extra preds in tier 1
                    new_t1_pct = diff1/t0_cnt *randint(99,100)/100
                    new_t2_pct = diff2/(t0_cnt-diff1) *randint(97,99)/100
                    new_t3_pct = diff3/(t0_cnt-diff1-diff2) *randint(97,99)/100
                    print(diff1/t0_cnt,diff2/(t0_cnt-diff1),diff3/(t0_cnt-diff1-diff2))
                    #print(new_t1_pct,new_t2_pct,new_t3_pct)

                    tier1[new_tier] = tier_extra # everyone above median of original prediction
                    f_chng.loc[f_chng[tiers_pred]==tier_chng1,new_tier] = tier_chng1
                    f_chng.loc[f_chng[tiers_pred]==tier_chng2,new_tier] = tier_chng2
                    f_chng.loc[f_chng[tiers_pred]==tier_chng3,new_tier] = tier_chng3

                    tier0_1 = tier0.sample(frac=new_t1_pct)
                    tier0_rem = tier0[~(tier0.index.isin(tier0_1.index))]
                    tier0_2 = tier0_rem.sample(frac=new_t2_pct)
                    tier0_rem = tier0_rem[~(tier0_rem.index.isin(tier0_2.index))]
                    tier0_3 = tier0_rem.sample(frac=new_t3_pct)
                    tier0_rem = tier0_rem[~(tier0_rem.index.isin(tier0_3.index))]

                    tier0_rem[new_tier] = tier_extra
                    tier0_1[new_tier] = tier_chng1
                    tier0_2[new_tier] = tier_chng2
                    tier0_3[new_tier] = tier_chng3

                    r = f_chng.append(tier1)
                    r = r.append(tier0_1)
                    r = r.append(tier0_2)
                    r = r.append(tier0_3)
                    r = r.append(tier0_rem)

            print(r[new_tier].value_counts())

            return r   

        # In[ ]:

        def score(full_df,final_vars_lob,clf_lob,final_vars_hsd,clf_hsd,final_vars_mixvid,clf_mixvid,final_vars_vid,clf_vid):

            df_main = datamap(full_df)
            fin_df = datascore_m(df_main, final_vars_lob, clf_lob, "lob_tiers_score", "lob") 
            print("fin_df",fin_df)
            print("LOB model done")
            """
            def tier():

                try:      

                  df = tier_adjust(df_lob, 0.4104, 0.1515,"lob_tiers_score", "lob", 2,3,1,"lob_tiers_pred") 
                  print("LOB calc done")
                  print(df.lob_tiers_pred.value_counts())

                  print ("dfff",df)


                  df1 = df[df['lob_tiers_pred']==1]
                  df2 = df[df['lob_tiers_pred']==2]
                  df3 = df[df['lob_tiers_pred']==3]
                  print ("s1")
                  #print(df2.lob_tiers_score.sum())
                  #print(df2.shape[0])


                  cols = df.columns 
                  print ("s2")
                  ### HSD Tiers multi###
                  df1 =datascore_m(df1,final_vars_hsd,clf_hsd,"hsd_tiers_score","hsd") 
                  print("HSD model done")
                  print(df1.hsd.value_counts())
                  print ("s3")
                  df1_1 = tier_adjust(df1, 0.6941, 0.1221,"hsd_tiers_score", "hsd", 1,3,2,"hsd_tiers_pred")
                  df1_1.drop(['hsd'], inplace=True, axis=1) 
                  df1_1.columns = list(cols) + ['score','pred']
                  print("HSD calc done")
                  print(df1_1.pred.value_counts())

                  ### Mix VID tiers TP ###
                  df3 = datascore_b(df3, final_vars_mixvid, clf_mixvid, "mixvid_tiers_score", "mixvid_tiers_pred")
                  df3.loc[df3['mixvid_tiers_pred']==0, 'vid_tiers_2'] = "Starter/Performance"
                  df3.loc[df3['mixvid_tiers_pred']==1, 'vid_tiers_2'] = "Preferred/Blast" 
                  df3.drop(['mixvid_tiers_pred'], inplace=True, axis=1)
                  df3.columns = list(cols) + ['score', 'pred'] 
                  print("Mixvid TP model done")
                  print(df3.pred.value_counts())

                  df3_1 = tier_adjust_2(df3, 0.268901,"score", "pred", "Preferred/Blast","Starter/Performance","tp_tiers_pred")
                  df3_1.drop(['pred'], inplace=True, axis=1) 
                  df3_1.columns = list(cols) + ['score','pred']
                  print("TP calc done")
                  print(df3_1.pred.value_counts())

                  ### VID tiers ###
                  df2_1 = datascore_b(df2, final_vars_vid, clf_vid, "vid_tiers_score", "vid_tiers_pred") 
                  df2_1.loc[df2_1['vid_tiers_pred']==0, 'vid_tiers_1'] = "Limted Basic/Blast"
                  df2_1.loc[df2_1['vid_tiers_pred']==1, 'vid_tiers_1'] = "Limited Basic/Performance" 
                  print("VID model done")
                  print(df2_1.vid_tiers_1.value_counts())

                  ### Mix VID tiers ###
                  df2_1 = datascore_b(df2_1, final_vars_mixvid, clf_mixvid, "mixvid_tiers_score", "mixvid_tiers_pred") 
                  df2_1.loc[df2_1['mixvid_tiers_pred']==0, 'vid_tiers_2'] = "Starter/Performance"
                  df2_1.loc[df2_1['mixvid_tiers_pred']==1, 'vid_tiers_2'] = "Preferred/Blast"
                  print("DP Mix vid done")
                  print(df2_1.vid_tiers_2.value_counts())

                  ### VID/HSD ###
                  df2_1['dp_score'] = np.where(df2_1['mixvid_tiers_score'] > df2_1['vid_tiers_score'], df2_1['mixvid_tiers_score'],df2_1['vid_tiers_score'])
                  df2_1['dp_tiers'] = np.where(df2_1['mixvid_tiers_score'] > df2_1['vid_tiers_score'], df2_1['vid_tiers_2'], df2_1['vid_tiers_1']) 

                  print("VID/HSD model done")
                  print(df2_1.dp_tiers.value_counts())

                  col = df2_1.columns
                  i = len(col)-8
                  col = list(col[:i]) + list(col[-2:])
                  df2_1 = df2_1[col]
                  df2_1.columns = list(cols) + ['score', 'pred']
                  print(df2_1.pred.value_counts())

                  df2_2 = tier_adjust_3(df2_1,0.28315,0.26372,0.17532,"score","pred","Limited Basic/Performance","Preferred/Blast",
                                        "Starter/Performance","Limted Basic/Blast","dp_tiers_pred")
                  df2_2.drop(['pred'], inplace=True, axis=1) 
                  df2_2.columns = list(cols) + ['score','pred']
                  print("DP calc done")
                  print(df2_2.pred.value_counts())

                  print("#### finally append ###")
                  #print(df1_1.shape[0])
                  #print(df2_2.shape[0])
                  #print(df3_1.shape[0])


                  return df1_1, df2_2, df3_1

                except Exception:
                  traceback.print_exc()
                  print("*** restart ***")
                  df1_f, df2_f, df3_f = tier()
                  return df1_f, df2_f, df3_f

            df1_f, df2_f, df3_f = tier()  

            fin_df = df1_f.append(df3_f)
            fin_df = fin_df.append(df2_f)
            #print(fin_df.shape[0])
            """
            fin_df.loc[(fin_df['lob']==1), 'pred_class'] = 'Internet,Performance'
            fin_df.loc[(fin_df['lob']==0), 'pred_class'] = 'Internet,Performance Starter'
            """
            fin_df.loc[(fin_df['lob_tiers_pred']==1) & (fin_df['pred']==2.0), 'pred_class'] = 'Internet,Performance Starter'
            fin_df.loc[(fin_df['lob_tiers_pred']==1) & (fin_df['pred']==3.0), 'pred_class'] = 'Internet,Blast'
            fin_df.loc[(fin_df['lob_tiers_pred']==2) & (fin_df['pred']=='Limited Basic/Performance'),'pred_class'] = 'TV_Internet,Limited Basic/Performance'
            fin_df.loc[(fin_df['lob_tiers_pred']==2) & (fin_df['pred']=='Limted Basic/Blast'),'pred_class'] = 'TV_Internet,Limted Basic/Blast'
            fin_df.loc[(fin_df['lob_tiers_pred']==2) & (fin_df['pred']=='Starter/Performance'),'pred_class'] = 'TV_Internet,Starter/Performance'
            fin_df.loc[(fin_df['lob_tiers_pred']==2) & (fin_df['pred']=='Preferred/Blast'),'pred_class'] = 'TV_Internet,Preferred/Blast'
            fin_df.loc[(fin_df['lob_tiers_pred']==3) & (fin_df['pred']=='Starter/Performance'),'pred_class'] = 'TV_Internet_Voice,Starter/Performance'
            fin_df.loc[(fin_df['lob_tiers_pred']==3) & (fin_df['pred']=='Preferred/Blast'),'pred_class'] = 'TV_Internet_Voice,Preferred/Blast'
            """
            fin_df['account'] = ''
            fin_df['accountid'] = ''
            fin_df['corp_sysprin'] = ''
            fin_df['housekey'] = ''
            fin_df['eps_acct_id'] = ''
            fin_df['eps_busn_id'] = ''
            fin_df['eps_addr_id'] = ''

            print("All done")
            print(fin_df['pred_class'].value_counts())

            return fin_df

        # In[ ]:

        def get_output(df):
            df.columns = [x.upper() for x in df.columns]
            df['GUID'] = 3
            df['SNAPSHOT_DATE'] = "2019-02-02"
            df['YEAR'] = 2019
            df['MONTH'] = 2
            df['MODEL_OUTPUT_ID'] = MODEL_ID
            df['MODEL_SCORE'] = ''
            df['MODEL_DECILE'] = ''
            df['MODEL_DECILE_REG'] = ''
            df['MODEL_DECILE_DIV'] = ''
            df['MODEL_SEGMENT'] = ''
            df['MODEL_CLASSIFICATION'] = df['PRED_CLASS']
            df['MODEL_CLASS'] = df['LOB']
            df['MODEL_INSERTION_DATE'] = date.today().strftime("%Y-%m-%d")
            df['MODEL_PREFIX'] = MODEL_PREFIX

            score_output = df.ix[:,[
                                'GUID',
                                'MODEL_CLASS',
                                'ACCOUNT',
                                'ACCOUNTID',
                                'CORP_SYSPRIN',
                                'HOUSEKEY',
                                'SNAPSHOT_DATE',
                                'YEAR',
                                'MONTH',
                                'MODEL_OUTPUT_ID',
                                'MODEL_PREFIX',
                                'MODEL_SCORE',
                                'MODEL_DECILE',
                                'MODEL_DECILE_REG',
                                'MODEL_DECILE_DIV',
                                'MODEL_SEGMENT',
                                'MODEL_CLASSIFICATION',
                                'MODEL_INSERTION_DATE',
                                'EPS_ACCT_ID',
                                'EPS_BUSN_ID',
                                'EPS_ADDR_ID']]
            
            score_output.columns = ['GUID',
                                'MODEL_CLASS'
                                ,'ACCOUNT_NUMBER'
                                ,'CUSTOMER_ACCOUNT_ID'
                                ,'CORP_SYSPRIN'
                                ,'HOUSE_KEY'
                                ,'SNAPSHOT_DATE'
                                ,'YEAR'
                                ,'MONTH'
                                ,'MODEL_OUTPUT_ID'
                                ,'MODEL_PREFIX'
                                ,'MODEL_SCORE'
                                ,'MODEL_DECILE'
                                ,'MODEL_DECILE_REG'
                                ,'MODEL_DECILE_DIV'
                                ,'MODEL_SEGMENT'
                                ,'MODEL_CLASSIFICATION'
                                ,'MODEL_INSERTION_DATE'
                                ,'EPS_ACCT_ID'
                                ,'EPS_BUSN_ID'
                                ,'EPS_ADDR_ID']
            
            return(score_output)


        # In[ ]:

        def run():
            #data = auto.read_table('EBI_N19_08_008_HIVE')
            data=pd.DataFrame(x,columns=["V1","V2","V3","V4","V5","V6","V7","V8","V9","V10"])

            clf_lob = joblib.load('models/lobmodel.pkl')
            clf_hsd = joblib.load('models/lobmodel.pkl')
            clf_mixvid = joblib.load('models/mixvid.pkl')
            clf_vid = joblib.load('models/vid.pkl')

            varb={}
            varb['Var_name']=["V1","V2","V3","V4","V5","V6","V7","V8","V9","V10"]

            #print(final_vars_lob,clf_lob,final_vars_hsd,clf_hsd,final_vars_mixvid,clf_mixvid,final_vars_vid,clf_vid)

            data.columns = [i[0] for i in data.columns.str.split('.')] 
            #final_vars_mixvid['Var_name'].loc[7] = 'd_cnt_desktop_visit_l7d'
            #final_vars_vid['Var_name'].loc[4] = 'd_sum_paid_search_last_l7d'



            df = score(data,varb,clf_lob,varb,clf_hsd,varb,clf_mixvid,varb,clf_vid) 


            score_output = get_output(df)
            return score_output
            #auto.write_file(score_output, 'EBI_N19_08_008') 

        results=run()
        print ("Resultss",results)
        return results

    def predict_proba(self, x):
        """
        This function is implemented if a probability is required instead of the class value for classification.

        :param: x: A list or list of list of input vector
        :return: A dict of predict probability of the model for
         each feature in the features list
        """
        pass

    def feature_list(self):
        """
        Required for online and offline predictions. This function binds the request data to the actual feature names.

        :param: None
        :return: A list of features
        """
        return ["V1","V2","V3","V4","V5","V6","V7","V8","V9","V10"]

