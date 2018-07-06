# Report-Functions
# -*- coding: utf-8 -*-
"""
Created on Fri Jul  6 09:14:06 2018

@author: xb
"""

from odps import ODPS
from odps.tunnel import TableTunnel
import pandas as pd
from pandas import DataFrame,Series



#1.func1:import data from odps
odps = ODPS('LTAI9DpuxobuOxHZ', 'AVrnebwIMmF9PiIKxS3HrzkaL4E1cL', 'gravity_quant',
            endpoint='http://service.cn.maxcompute.aliyun.com/api')

def download_by_tunnel(table_name, file_path, row_count, pt=None, sep='@###@'):
    """
    通过dataframe的方式读取odps的表数据
    :param table_name:
    :param file_path:
    :return:
    """

    tunnel = TableTunnel(odps)
    if pt is not None:
        download_session = tunnel.create_download_session(table_name, partition_spec=pt)
    else:
        download_session = tunnel.create_download_session(table_name)
    with open(file_path, 'w') as f:
        with download_session.open_record_reader(0, download_session.count) as reader:
            for record in reader:
                line = ''
                for i in range(row_count):
                    if i > 0:
                        line = line + sep
                    line = line + str(record[i])
                line = line + '\n'
                f.writelines(line)






#2.func2:get the accumulated return
def get_acreturn(df_initial):
    df_initial=df_initial.dropna()
    
    # define df_initial_return to add accumulated return to the dataframe
    df_initial_return=pd.concat([df_initial, pd.DataFrame(columns=['ri7','rm7','rd7',
                                                             'ri14','rm14','rd14',
                                                             'ri30','rm30','rd30',
                                                             'ri60','rm60','rd60',
                                                             'ri90','rm90','rd90'])])
    
    #use loop to apply to each row of the data
    
    #define a function to calculate accumulated ln-return
    #return 3 kinds of return and store in a list(stock_return:0;market_return:1;industry_return:2)
    def ac_return(df,start,end):
        r=[0,0,0]
        stock_return_sum=0
        market_return_sum=0
        industry_return_sum=0
        for i in range(df.iloc[:,0].size):
            if df['label'].iloc[i]>=start and df['label'].iloc[i]<=end:
                stock_return_sum+=float(df['stock_return'].iloc[i])
                market_return_sum+=float(df['market_return'].iloc[i])
                industry_return_sum+=float(df['industry_return'].iloc[i])
        r[0]=stock_return_sum
        r[1]=market_return_sum
        r[2]=industry_return_sum
        return r
    
    
    def df_return_befday(df_initial_return):
        #to label the trade_date numbers according to date of report    
        trade_date=Series(str(df_initial_return['trade_date_comb']).split('#'))
        stock_return=Series(str(df_initial_return['stock_return_comb']).split('#'))
        market_return=Series(str(df_initial_return['market_return_comb']).split('#'))
        industry_return=Series(str(df_initial_return['industry_return_comb']).split('#'))
    
        # combine 4 series to dataframe
        df=pd.concat([trade_date,stock_return,market_return,industry_return],axis=1,
                            keys=['trade_date','stock_return','market_return','industry_return'])
        df_return=pd.concat([df, pd.DataFrame(columns=['label'])])
        # sort by trade_date
        df_return_sort=df_return.sort_values(by ='trade_date',axis = 0,ascending = True)
        
        #compare trade_date to report date and calculate the trade_date numbers
        #label 1 to n first and then subtract the before_event_day numbers
        num=1
        bef_day=0
        for i in range(df_return_sort.iloc[:,0].size):
            df_return_sort['label'].iloc[i]=num
            num=num+1
            if df_return_sort['trade_date'].iloc[i]<=df_initial_return['date']:
                bef_day+=1
        df_return_sort['label']=df_return_sort['label'].apply(lambda x:x-bef_day)
    
    
        #use the function to calculate different periods of return and append to df_initial
        #add new columns to df_initial
        #ri represent stock_return,rm represent market_return,rd represent industry_return
        r7=ac_return(df_return_sort,1,7)
        r14=ac_return(df_return_sort,1,14)
        r30=ac_return(df_return_sort,1,30)
        r60=ac_return(df_return_sort,1,60)
        r90=ac_return(df_return_sort,1,90)
    
        df_initial_return['ri7']=r7[0]
        df_initial_return['rm7']=r7[1]
        df_initial_return['rd7']=r7[2]
        df_initial_return['ri14']=r14[0]
        df_initial_return['rm14']=r14[1]
        df_initial_return['rd14']=r14[2]
        df_initial_return['ri30']=r30[0]
        df_initial_return['rm30']=r30[1]
        df_initial_return['rd30']=r30[2]
        df_initial_return['ri60']=r60[0]
        df_initial_return['rm60']=r60[1]
        df_initial_return['rd60']=r60[2]
        df_initial_return['ri90']=r90[0]
        df_initial_return['rm90']=r90[1]
        df_initial_return['rd90']=r90[2]
        print('one row is OK!')
        
        return df_initial_return
    
    
    #use the function  df_return_befday(df_initial_return)
    df_initial_return2=df_initial_return.apply(lambda x:df_return_befday(x),axis=1)
    
    
    
    
    
    #add rm_diff(*5) and rd_diff(*5) to represent ri-rm and ri-rd.
    df_initial_r_diff=pd.concat([df_initial_return2, pd.DataFrame(columns=['rm7_diff','rd7_diff',
                                                                          'rm14_diff','rd14_diff',
                                                                          'rm30_diff','rd30_diff',
                                                                          'rm60_diff','rd60_diff',
                                                                          'rm90_diff','rd90_diff'])])
    def diff(df_initial_r_diff):
        df_initial_r_diff['rm7_diff']=df_initial_r_diff['ri7']-df_initial_r_diff['rm7']
        df_initial_r_diff['rd7_diff']=df_initial_r_diff['ri7']-df_initial_r_diff['rd7']
        df_initial_r_diff['rm14_diff']=df_initial_r_diff['ri14']-df_initial_r_diff['rm14']
        df_initial_r_diff['rd14_diff']=df_initial_r_diff['ri14']-df_initial_r_diff['rd14']
        df_initial_r_diff['rm30_diff']=df_initial_r_diff['ri30']-df_initial_r_diff['rm30']
        df_initial_r_diff['rd30_diff']=df_initial_r_diff['ri30']-df_initial_r_diff['rd30']
        df_initial_r_diff['rm60_diff']=df_initial_r_diff['ri60']-df_initial_r_diff['rm60']
        df_initial_r_diff['rd60_diff']=df_initial_r_diff['ri60']-df_initial_r_diff['rd60']
        df_initial_r_diff['rm90_diff']=df_initial_r_diff['ri90']-df_initial_r_diff['rm90']
        df_initial_r_diff['rd90_diff']=df_initial_r_diff['ri90']-df_initial_r_diff['rd90']
        return df_initial_r_diff
    
    df_initial_r_diff2=df_initial_r_diff.apply(lambda x:diff(x),axis=1)
    
    # now dataframe df_initial_r_diff contains all the information.
    
    #reindex the column in same order
    df_initial_r_diff2 = df_initial_r_diff2[['code','name','date','level','researcher','source','industry',
                                           'title','content','industry_return_comb','market_return_comb',
                                           'stock_return_comb','trade_date_comb','ri7','rm7','rd7','ri14',
                                           'rm14','rd14','ri30','rm30','rd30','ri60','rm60','rd60','ri90',
                                           'rm90','rd90','rm7_diff','rd7_diff','rm14_diff','rd14_diff',
                                           'rm30_diff','rd30_diff','rm60_diff','rd60_diff','rm90_diff','rd90_diff']]
    return df_initial_r_diff2







#3.func3:input the code and date,output the accumulated return
def code_return(code,date,df):
    output=pd.DataFrame(index=['30 days','60 days','90 days'],columns=['r','rm_diff','rd_diff'])
    df0=df[df['code']==code]
    df1=df0[df0['date']==date]
    output['r']=[df1['ri30'].iloc[0],df1['ri60'].iloc[0],df1['ri90'].iloc[0]]
    output['rm_diff']=[df1['rm30_diff'].iloc[0],df1['rm60_diff'].iloc[0],df1['rm90_diff'].iloc[0]]
    output['rd_diff']=[df1['rd30_diff'].iloc[0],df1['rd60_diff'].iloc[0],df1['rd90_diff'].iloc[0]]
    print(output)





#4.func4:imput month and code,output the level_count for each code in 30days,60days and 90days
def level_count(code,month1,month2,month3,df):
    #get month of the date
    df.dropna()
    df['date']=df['date'].astype('datetime64')
    df['date_month'] = df['date'].map(lambda x: x.strftime("%Y%m"))
    #add 'level_d' to classify into negative(-1),medium(0) and positive(1)
    def level_d(x):
        if(x=='买入' or x=='增持'):
            a=1
        elif x=='中性':
            a=0
        else:
            a=-1
        return a
    
    df['level_d'] = df['level'].map(lambda x: level_d(x))
    #get level_count for each month and each code
    df_level=df.groupby(['date_month','industry','code'])['level_d'].value_counts()
    df_level=DataFrame(df_level)
    df_level.columns=['level_count']
    df_level=df_level.reset_index()
    #create df_output for output
    df_output=pd.DataFrame(index=['30 days','60 days','90 days'],columns=['正向','中性','负向'])
    #match the code and month1/month2/month3
    df0=df_level[df_level['code']==code]
    #for month1
    df1=df0[df0['date_month']==month1]
    df_nega1=df1[df1['level_d']==-1]
    df_mid1=df1[df1['level_d']==0]
    df_posi1=df1[df1['level_d']==1]
    #for month2
    df2=df0[df0['date_month']==month2]
    df_nega2=df2[df2['level_d']==-1]
    df_mid2=df2[df2['level_d']==0]
    df_posi2=df2[df2['level_d']==1]
    #for month3
    df3=df0[df0['date_month']==month3]
    df_nega3=df3[df3['level_d']==-1]
    df_mid3=df3[df3['level_d']==0]
    df_posi3=df3[df3['level_d']==1]
    #if df is empty,then fill it with 0
    dfzero=pd.DataFrame({'date_month':[0],'industry':[0],'code':[0],'level_d':[0],'level_count':[0]})
    if(df_nega1.size==0):
        df_nega1=dfzero
    if(df_posi1.size==0):
        df_posi1=dfzero
    if(df_mid1.size==0):
        df_mid1=dfzero
    if(df_nega2.size==0):
        df_nega2=dfzero
    if(df_posi2.size==0):
        df_posi2=dfzero
    if(df_mid2.size==0):
        df_mid2=dfzero
    if(df_nega3.size==0):
        df_nega3=dfzero
    if(df_posi3.size==0):
        df_posi3=dfzero
    if(df_mid3.size==0):
        df_mid3=dfzero
    
    #fill the df_output with the matching number
    df_output.loc['30 days']=[df_posi1['level_count'].iloc[0],df_mid1['level_count'].iloc[0],df_nega1['level_count'].iloc[0]]
    df_output.loc['60 days']=[df_posi1['level_count'].iloc[0]+df_posi2['level_count'].iloc[0],
                              df_mid1['level_count'].iloc[0]+df_mid2['level_count'].iloc[0],
                              df_nega1['level_count'].iloc[0]+ df_nega2['level_count'].iloc[0]]
    df_output.loc['90 days']=[df_posi1['level_count'].iloc[0]+df_posi2['level_count'].iloc[0]+df_posi3['level_count'].iloc[0],
                              df_mid1['level_count'].iloc[0]+df_mid2['level_count'].iloc[0]+df_mid3['level_count'].iloc[0],
                              df_nega1['level_count'].iloc[0]+ df_nega2['level_count'].iloc[0]++df_nega3['level_count'].iloc[0]]
    print(df_output)

    

    


#5.func5:input month,code and industry, then output correct rate(30days:ri/rm_diff/rd_diff) in positive level/negative level/medium level
def correct_rate(source,month,industry,df):
    df_all_test=df
    df_all_test.dropna()
    df_all_test['date']=df_all_test['date'].astype('datetime64')
    #get month and year of the date
    df_all_test=pd.concat([df_all_test, pd.DataFrame(columns=['date_month'])])
    df_all_test['date_month'] = df_all_test['date'].map(lambda x: x.strftime("%Y%m"))
    df_all_test=pd.concat([df_all_test, pd.DataFrame(columns=['date_year'])])
    df_all_test['date_year'] = df_all_test['date'].map(lambda x: x.strftime("%Y"))
    #df_all_test中券商名字进行调整
    def source_adjust(x):
        if (x=='中国银河'):
            x='中国银河证券'
        elif(x=='国泰君安'):
            x='国泰君安证券'
        elif(x=='宏源证券'):
            x='申万宏源'
        elif(x=='北京高华'):
            x='高盛高华'
        elif(x=='中银国际'):
            x='中银国际证券'
        elif(x=='群益证券'):
            x='群益证券(香港)'
        return x
    
    df_all_test['source']=df_all_test.apply(lambda x: source_adjust(x['source']),axis=1)
    
    df_source_info=df_all_test[['date_year','industry','source','code','level','ri30',
                                'rm30_diff','rd30_diff','ri60','rm60_diff','rd60_diff',
                               'ri90','rm90_diff','rd90_diff']]
    
    #define level_d to describe level is positive or medium or negative
    def level_d(x):
        if(x=='买入' or x=='增持'):
            a=1
        elif x=='中性':
            a=0
        else:
            a=-1
        return a
    
    df_source_info['level_d'] = df_source_info['level'].map(lambda x: level_d(x))
    
    #groupby level_d into 3 groups
    df_source_posi=df_source_info[df_source_info['level_d']==1]
    df_source_mid=df_source_info[df_source_info['level_d']==0]
    df_source_nega=df_source_info[df_source_info['level_d']==-1]
    
    list_level_d=[df_source_posi,df_source_mid,df_source_nega]
    #for each group, calculate the source_count and correct_rate
    #define correct_num to determine whether the ac_return is in accordance with level_d
    def correct_num(level_d,ac_return):
        c=0
        if (level_d>0 and ac_return>0) or (level_d<=0 and ac_return<0):
            c=1
        return c
    
    #calculate the source_count and correct_number for each group
    for df in list_level_d:
        #initialize
        df_source=DataFrame()
        df_source_correct_ini=DataFrame()
        df_source_correct=DataFrame()
        df_source_correct_rate=DataFrame()   
        #source_count
        df_source=DataFrame(df.groupby(['date_year','industry'])['source'].value_counts())
        df_source.columns=['source_count']
        df_source=df_source.reset_index()
        #correct_number
        df['correct_30i']=df.apply(lambda x:correct_num(x['level_d'],x['ri30']),axis=1)
        df['correct_30m']=df.apply(lambda x:correct_num(x['level_d'],x['rm30_diff']),axis=1)
        df['correct_30d']=df.apply(lambda x:correct_num(x['level_d'],x['rd30_diff']),axis=1)
        df['correct_60i']=df.apply(lambda x:correct_num(x['level_d'],x['ri60']),axis=1)
        df['correct_60m']=df.apply(lambda x:correct_num(x['level_d'],x['rm60_diff']),axis=1)
        df['correct_60d']=df.apply(lambda x:correct_num(x['level_d'],x['rd60_diff']),axis=1)
        df['correct_90i']=df.apply(lambda x:correct_num(x['level_d'],x['ri90']),axis=1)
        df['correct_90m']=df.apply(lambda x:correct_num(x['level_d'],x['rm90_diff']),axis=1)
        df['correct_90d']=df.apply(lambda x:correct_num(x['level_d'],x['rd90_diff']),axis=1)
    
        #groupby date_year,industry,source to calculate the sum number of correct
        df_source_correct_ini=df[['date_year','industry','source','level_d','correct_30i','correct_30m','correct_30d',
                                  'correct_60i','correct_60m','correct_60d','correct_90i','correct_90m','correct_90d']]
        df_source_correct=DataFrame(df_source_correct_ini.groupby(['date_year','industry','source','level_d']).sum()).reset_index()
        #merge df_source(source_count) and df_source_correct(correct_30m/30d) and so on
        df_source_correct_rate=df_source.merge(df_source_correct,how='left',on=['date_year','industry','source'])
        #calculate the correct rate(*100)
        df_source_correct_rate['correct_rate30i']=100*df_source_correct_rate['correct_30i']/df_source_correct_rate['source_count']  
        df_source_correct_rate['correct_rate30m']=100*df_source_correct_rate['correct_30m']/df_source_correct_rate['source_count']  
        df_source_correct_rate['correct_rate30d']=100*df_source_correct_rate['correct_30d']/df_source_correct_rate['source_count']  
        df_source_correct_rate['correct_rate60i']=100*df_source_correct_rate['correct_60i']/df_source_correct_rate['source_count']  
        df_source_correct_rate['correct_rate60m']=100*df_source_correct_rate['correct_60m']/df_source_correct_rate['source_count']  
        df_source_correct_rate['correct_rate60d']=100*df_source_correct_rate['correct_60d']/df_source_correct_rate['source_count']  
        df_source_correct_rate['correct_rate90i']=100*df_source_correct_rate['correct_90i']/df_source_correct_rate['source_count']  
        df_source_correct_rate['correct_rate90m']=100*df_source_correct_rate['correct_90m']/df_source_correct_rate['source_count']  
        df_source_correct_rate['correct_rate90d']=100*df_source_correct_rate['correct_90d']/df_source_correct_rate['source_count']  
    
        #sort by year,industry and correct_rate
        df_source_correct_rate=df_source_correct_rate.sort_values(by=['date_year','industry','source_count','correct_rate30d'],ascending=[True,True,False,False])
        #export
        if df_source_correct_rate['level_d'].iloc[0]==1:
            df_source_rate_positive=df_source_correct_rate
        elif df_source_correct_rate['level_d'].iloc[0]==0:
            df_source_rate_medium=df_source_correct_rate
        else:
            df_source_rate_negative=df_source_correct_rate
    
    #output 
    df0=df_source_rate_positive[df_source_rate_positive['source']==source]
    df00=df0[df0['industry']==industry]
    df_out_posi=df00[df00['date_year']==str(int(month)//100-1)]
    df1=df_source_rate_medium[df_source_rate_medium['source']==source]
    df11=df1[df1['industry']==industry]
    df_out_mid=df11[df11['date_year']==str(int(month)//100-1)]
    df2=df_source_rate_negative[df_source_rate_negative['source']==source]
    df22=df2[df2['industry']==industry]
    df_out_nega=df22[df22['date_year']==str(int(month)//100-1)]
    
    #if df_out_posi/df_out_posi/df_out_posi is empty,fill it with 'nan'
    dfnan=pd.DataFrame({'correct_rate30i':['nan'],'correct_rate30m':['nan'],'correct_rate30d':['nan']})
    if df_out_posi.size==0:
        df_out_posi=dfnan
    if df_out_mid.size==0:
        df_out_mid=dfnan
    if df_out_nega.size==0:
        df_out_nega=dfnan
    #define df_output
    df_output=pd.DataFrame(index=['rate_ri','rate_rm_diff','rate_rd_diff'],columns=['正向','中性','负向'])
    df_output['正向']=[df_out_posi['correct_rate30i'].iloc[0],df_out_posi['correct_rate30m'].iloc[0],df_out_posi['correct_rate30d'].iloc[0]]
    df_output['中性']=[df_out_mid['correct_rate30i'].iloc[0],df_out_mid['correct_rate30m'].iloc[0],df_out_mid['correct_rate30d'].iloc[0]]
    df_output['负向']=[df_out_nega['correct_rate30i'].iloc[0],df_out_nega['correct_rate30m'].iloc[0],df_out_nega['correct_rate30d'].iloc[0]]
    print(df_output)
   







#6.main
if __name__ == '__main__':
    #func1
    download_by_tunnel('tdl_quant_intern_report7_combine2017','C:/Users/xb/Desktop/2017.txt',13)  
    
    #func2:get accumulated return for all data
    df_initial=pd.read_csv('C:/Users/xb/Desktop/report/origin/data_report7cmb_2017.txt',sep='@###@',
                   names=['code','name','title','level','source','researcher','date',
                          'trade_date_comb','industry','stock_return_comb','market_return_comb',
                          'industry_return_comb','content'],dtype={'code':str})
    df_initial_return=get_acreturn(df_initial)
    df_initial_return.to_csv('C:/Users/xb/Desktop/report/new3_leveld/data_report7cmb_2017_new3.txt',encoding='utf-8', index=False)
    
    #define df_initial_return_all
    df1301=pd.read_csv('C:/Users/xb/Desktop/report/new2/data_report7cmb_201301_new2.txt',sep=',',
                       dtype={'code':str})
    df1302=pd.read_csv('C:/Users/xb/Desktop/report/new2/data_report7cmb_201302_new2.txt',sep=',',
                       dtype={'code':str})
    df1401=pd.read_csv('C:/Users/xb/Desktop/report/new2/data_report7cmb_201401_new2.txt',sep=',',
                       dtype={'code':str})
    df1402=pd.read_csv('C:/Users/xb/Desktop/report/new2/data_report7cmb_201402_new2.txt',sep=',',
                       dtype={'code':str})
    df1501=pd.read_csv('C:/Users/xb/Desktop/report/new2/data_report7cmb_201501_new2.txt',sep=',',
                       dtype={'code':str})
    df1502=pd.read_csv('C:/Users/xb/Desktop/report/new2/data_report7cmb_201502_new2.txt',sep=',',
                       dtype={'code':str})
    df16=pd.read_csv('C:/Users/xb/Desktop/report/new3_leveld/data_report7cmb_2016_new3.txt',sep=',',
                       dtype={'code':str})
    df17=pd.read_csv('C:/Users/xb/Desktop/report/new3_leveld/data_report7cmb_2017_new3.txt',sep=',',
                       dtype={'code':str})
    df18=pd.read_csv('C:/Users/xb/Desktop/report/new3_leveld/data_report7cmb_2018_new3.txt',sep=',',
                       dtype={'code':str})
    
    df_initial_return_all=pd.concat([df1301,df1302,df1401,df1402,df1501,df1502,df16,df17,df18], axis=0)
    
    #func3:input the code,date and df,output the accumulated return
    code_return('000001','2017-03-15 00:00:00',df_initial_return_all)
    
    #func4:imput month and code,output the level_count for each code in 30days,60days and 90days
    level_count('000089','201701','201702','201703',df_initial_return_all)
    
    #func5:input month,code and industry, then output correct rate(30days:ri/rm_diff/rd_diff) in positive level/negative level/medium level
    correct_rate('申万宏源','201401','交通运输',df_initial_return_all)





























































