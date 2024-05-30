import numpy
import pandas as pd
import duckdb
import sqlite3
import copy
from tkinter import filedialog

class_file_path = filedialog.askopenfilename(title='请选择动作编号表',filetypes=[('excel files(*.xlsx)','*.xlsx')])
data_file_path = filedialog.askopenfilename(title='请选择数据表',filetypes=[('excel files(*.xlsx)','*.xlsx')])
dbFile = filedialog.asksaveasfilename(title='请选择数据库存储位置',filetypes=[('duckdb数据库文件(*.duckdb)','.duckdb')],
                                      defaultextension='.duckdb')
conn = duckdb.connect(dbFile)
# dbFile = filedialog.asksaveasfilename(title='请选择数据库存储位置',filetypes=[('sqlite数据库文件(*.db)','.db')],
#                                       defaultextension='.db')
# conn = sqlite3.connect(dbFile)
#该部分用于读取动作编号表
df = pd.read_excel(io=class_file_path)
df['过程名称'] = df['过程名称'].ffill()
df['序号'] = df['序号'].ffill()
id_times_dict = {}
class_row_data = []
for i in df.iloc:
    data = []
    for j in i:
        data.append(j)
    class_row_data.append(copy.deepcopy(data))
#该部分用于读取iedb数据
df = pd.read_excel(io=data_file_path)
col_name = ['时间','编号','名称','输入/输出']
row_data = []
for i in df.iloc:
    data = []
    for j in i:
        data.append(j)
    row_data.append(copy.deepcopy(data))
#将数据插入表中
#建表
cursor = conn.cursor()
SQL = ('CREATE TABLE IF NOT EXISTS action_id_table('
       'class_id_map int not null,'
       'name text not null,'
       'id int not null unique,'
       'times int not null);')
cursor.execute(SQL)
conn.commit()
SQL = ('CREATE TABLE IF NOT EXISTS action('
       'time int not null,'
       'id int not null,'
       'io text not null);')
cursor.execute(SQL)
conn.commit()
#建立动作id到过程id的映射字典
action_name_dict = {}
action_class_id_dict = {}
action_times_dict = {}
for row in class_row_data:
    class_id = int(row[0])
    class_name = str(row[1])
    name = str(row[2])
    start_id = int(row[3])
    end_id = row[4]
    bit = class_id - 1
    if pd.isna(end_id):
        action_name_dict[start_id] = name
        action_times_dict[start_id] = 0
        if action_class_id_dict.get(start_id) is None:
            action_class_id_dict[start_id] = 1 << bit
        else:
            action_class_id_dict[start_id] |= 1 << bit
    else:
        action_name_dict[start_id] = name + '（开始）'
        action_times_dict[start_id] = 0
        if action_class_id_dict.get(start_id) is None:
            action_class_id_dict[start_id] = 1 << bit
        else:
            action_class_id_dict[start_id] |= 1 << bit
        action_name_dict[end_id] = name + '（结束）'
        action_times_dict[end_id] = 0
        if action_class_id_dict.get(end_id) is None:
            action_class_id_dict[end_id] = 1 << class_id
        else:
            action_class_id_dict[end_id] |= 1 << class_id
#插入数据
for row in row_data:
    time = int(row[0])
    id = int(row[1])
    io = str(row[3])
    action_times_dict[id] = action_times_dict[id] + 1
    SQL = "INSERT INTO action VALUES({0},{1},'{2}');".format(time, id,  io)
    cursor.execute(SQL)
conn.commit()
for action in action_name_dict.items():
    id = action[0]
    name = action[1]
    class_id_map = action_class_id_dict[id]
    times = action_times_dict[id]
    SQL = "insert into action_id_table values({0},'{1}',{2},{3});".format(class_id_map,name,id,times)
    cursor.execute(SQL)
conn.commit()




SQL = "CREATE INDEX i_time ON action (time);"
cursor.execute(SQL)
conn.commit()
SQL = "CREATE INDEX i_class_id_map ON action_id_table (class_id_map);"
cursor.execute(SQL)
conn.commit()
SQL = "CREATE INDEX i_class_name ON action_id_table (name);"
cursor.execute(SQL)
conn.commit()
SQL = "CREATE INDEX i_class_id_map_in_action_id_table ON action_id_table (class_id_map);"
cursor.execute(SQL)
conn.commit()



cursor.close()
conn.close()