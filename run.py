import pandas as pd
import numpy as np
from glob import glob
import re
from os.path import join
from time import time

# Дефолтное кол-во людей на постах
NUMS_ON_POSTS = {'1 смена':12, '2 смена':13, 'Пригород':4,
                 'Старый павильон':11, 'Новый павильон':11, '15-ый пост':9}

dict_encoder = {0:'1 смена',1:'2 смена',2:'Старый павильон',
                3:'Новый павильон',4:'15-ый пост',5:'Пригород'}
reversed_dict_encoder = {'1 смена':0,'2 смена':1,'Старый павильон':2,
                'Новый павильон':3,'15-ый пост':4,'Пригород':5}

# Функция, помоающая искать баги
def search_for_bags(some_list):
    bags = []
    for i in some_list:
        if len(i)<=3:
            bags.append(i)
    for i in bags:
        some_list.remove(i)
    
    temp = some_list.copy()
    some_list=[]
    for i in temp:
        some_list.append(i.strip())
        
# Достаем список сотрудников
with open (join(glob('Data/')[0], 'ПОЛНЫЙ_СПИСОК_СОТРУДНИКОВ.txt'), 'r') as f:
    raw_list = f.read()
list_employees = raw_list.split('\n')
search_for_bags(list_employees)

# откроем таблицу с данными 
try:
    employees = pd.read_csv(glob('Data/employees.csv')[0])
except:
    employees = pd.DataFrame(columns=['timestamp']+list_employees, dtype=int)
    for i in range(4):
        timestamp=time()-345600
        employees.loc[i,:] =  [timestamp]+np.random.permutation([0 for x in range(12)]+\
                                                                [1 for x in range(13)]+\
                                                                [2 for x in range(11)]+\
                                                                [3 for x in range(11)]+\
                                                                [4 for x in range(9)]+ \
                                                                [5 for x in range(4)]).tolist()
    employees = employees.astype(int)
    employees.to_csv(join(glob('Data/')[0], 'employees.csv'), index=False)
    
    
    
# Достаем список старшаков
elders = {}
with open (glob(join(glob('Data/')[0], 'ПОСТОЯННО_НА_ОДНОЙ_ТОЧКЕ.txt'))[0], 'r') as f:
    raw_list = f.read()
pattern = re.compile("-*-\n")

raw_elders = pattern.split(raw_list)
search_for_bags(raw_elders)
num_elders = 0
for i in raw_elders:
    values = i.split(':')
    temp = values[1].split('\n')
    search_for_bags(temp)
    num_elders+=len(temp)
    
    elders[values[0]] = temp


# Удалим старшаков из списка для сортировки
today_list=list_employees.copy()
for i in elders.values():
    for j in i:
        today_list.remove(j)
        
# Достаем список отсутствующих
with open (glob('ОТСУТСТВУЮЩИЕ.txt')[0], 'r') as f:
    raw_list = f.read()
list_absent = raw_list.split('\n')
search_for_bags(list_absent)

# Удаляем отсутствующих из списка
for i in list_absent:    
    for j in list_employees:
        if re.match(j,i)!=None:
            today_list.remove(j)
            
# Если сотрудников удалили или добавили - внесем изменения в таблицу
fired = []
for i in employees.columns:
    if (i not in list_employees)&(i!='timestamp'):
        fired.append(i)
employees.drop(columns=fired, inplace=True)

for i in list_employees:
    if (i not in employees.columns):
        employees.loc[:,i]=9
        
# если между запросами прошло меньше 2 дней, то оставляем только последний
if (time()-employees.iloc[-1].timestamp)<=172800:
    employees.drop(index=employees.index[-1], inplace=True)
# Теперь смотрим кто работает сегодня и кого можно сортировать
today_employees = employees.loc[:,today_list]

# Сколько требуется на постах
demanded = pd.DataFrame(columns=['place', 'num'])
for i in range(6):
    demanded.loc[i,'place'] = dict_encoder[i]
    demanded.loc[i,'num'] = NUMS_ON_POSTS[dict_encoder[i]]
demanded = demanded.sort_values(by='num', ascending=False)

# Теперь определим откуда будем выдергивать в случае появления отсутствующих
def distribution(demanded, today_employees, num_elders, reversed_dict_encoder, elders):
    absent = demanded.num.sum() - today_employees.shape[1] - num_elders
    
    # удаляем отсутствующих
    demanded['present']=demanded['num']
    for i in range(absent):
        if (i==5):
            break
        demanded.loc[i,'present']=demanded.loc[i,'num']-1
        
    if (absent>=6):
        for i in range(absent-5):
            if (i==5):
                break
            demanded.loc[i,'present']-=1
    if (absent>=11):
        for i in range(absent-10):
            demanded.loc[i,'present']-=1

    
    
    # удаляем старшаков и постоянных
    demanded['to_process']=demanded['present']
    for key,value in elders.items():
        demanded.loc[reversed_dict_encoder[key],'to_process'] -= len(elders[key])
    
    # теперь получаем базовое распределение
    distribution=[]
    for i in range(6):
        distribution = distribution+[i for x in range(demanded.loc[i,'to_process'])]
    return distribution

distr = distribution(demanded, today_employees, num_elders, reversed_dict_encoder, elders)

# Функция подсчета лучшей комбинации
def calculate_best_opt(today_employees, distribution, num_iters=100000):
    last_1 = today_employees.iloc[-1].values
    last_2 = today_employees.iloc[-2].values
    last_3 = today_employees.iloc[-3].values
    last_4 = today_employees.iloc[-4].values
    
    opts = pd.DataFrame(columns=['opt','rate'])
    temp = []
    for i in range(num_iters):    
        opt = np.random.permutation(distribution)
        temp.append(opt)
    opts['opt'] = temp
    
    rates = []
    for row in opts.opt.iteritems():
        opt=row[1]
        rate = 27*np.count_nonzero((opt-last_1)==0) +\
               9*np.count_nonzero((opt-last_2)==0) +\
               3*np.count_nonzero((opt-last_3)==0) +\
               1*np.count_nonzero((opt-last_4)==0)
        rates.append(rate)
    opts['rate'] = rates
    opts = opts.sort_values(by='rate')
        
    return opts.iloc[0].opt

result = calculate_best_opt(today_employees, distr)



# Заносим результат в employees, ставим timestamp 
today_employees.loc[today_employees.index[-1]+1] = result.tolist()

employees.loc[employees.index[-1]+1]=[9 for x in range(employees.shape[1])]

employees.iloc[-1]['timestamp'] = time()

for col in today_employees.columns:
    employees.iloc[-1][col] = today_employees.iloc[-1][col]
        
    
# вносим постоянных в тот же список
temp = {}
for i in elders.keys():
    temp[reversed_dict_encoder[i]] = elders[i]

for num,names in temp.items():
    for name in names:
        employees.iloc[-1][name] = num
        
to_outcome = employees.iloc[-1].drop('timestamp', axis=0)

with open(glob('РЕЗУЛЬТАТ.txt')[0], 'w+') as f:
    for i in range(6):
        place = dict_encoder[i]
        people = list(to_outcome[to_outcome==i].index)
        f.write(place+':\n')
        for j in people:
            f.write(j+'\n')
        f.write('------------\n')

        
employees.to_csv(glob('Data/employees.csv')[0], index=False)
