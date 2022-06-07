# !usr/bin/env python
# -*- coding:utf-8 _*-
"""
@Author:huanglj
@File:dmdb-exporter.py
@Time:2021/2/25 11:05

#Prometheus
http://localhost:9090/

#grafana
http://127.0.0.1:3000/


#dmdb exporter:
http://127.0.0.1:8000/


"""
import time
from prometheus_client import Counter, Gauge,start_http_server,Info
import dmPython

###lixora: debug flag
global debug
debug=0

###dmdb connect config_info
user_name = "SYSDBA"
passwd = "sysdba1991"
server_name = "127.0.0.1"
server_port = 5236


dmdb_exporter_copyrights="\n ### Starting dmdb_exporter v2021.3.1 --support：lixora@foxmail.com  \n"



dbtype=str(server_name)+'_'+str(server_port)
#print(dbtype)

sessionstat= Gauge('session_stat','diffferent stat session all in one',['type','DBINFO'])
dmlstat= Gauge('dml_stat','diffferent sql status all in one',['type','DBINFO'])
loadstat= Gauge('load_stat','diffferent dbtime/cputime/iotime load all in one',['type','DBINFO'])
tpsstat=Gauge('tps_stat','transaction per second',['type','DBINFO'])

# dbname=Gauge('dbname','db name',['dbname','DBINFO'])
# db_arch_mode=Gauge('db_arch_mode','db archive mode',['archmode','DBINFO'])
# db_uptime=Gauge('db_uptime','db up time',['uptime','DBINFO'])
# db_version=Gauge('db_version','db version',['dbversion','DBINFO'])



#base = Info('dmdbversion', 'Description of info')
#base.info({'数据库名':'lixora','数据库版本': '1.2.3', '数据库端口': '8888','归档模式':'N','数据库启动时间':'aaa'})

tbs = Gauge('tbs','tablespace use info',['tbsname','type','tbs_max_MB','tbs_size_MB','tbs_used_MB','tbs_free_MB','tbs_used_percent','tbs_ext_Used_percent','DBINFO'])







sql_session='''
 (select para_name name,para_value value  from v$dm_ini where para_name='MAX_SESSIONS') 
  union
  (SELECT state name ,COUNT(*)  value FROM SYS.V$SESSIONS  group by state)
  union
  (SELECT 'Current SESSION',COUNT(*) SESSIONCOUNT  FROM SYS.V$SESSIONS)
'''
sql_dml='''
select name,stat_val from v$SYSSTAT where name in ('select statements','insert statements','delete statements','update statements')
'''
sql_load='''
select name,stat_val from v$SYSSTAT where name in ('DB time(ms)','CPU time(ms)','io wait time(ms)')
'''
sql_tbs='''SELECT  d.tablespace_name "Name",
       d.contents "Type",
       to_char(nvl(a.bytes / 1024 / 1024, 0), '99999999.9') "Total Ext Size (M)",
       to_char(nvl(a.bytes2 / 1024 / 1024, 0), '99999999.9') "Total Size (M)",
       to_char(nvl(a.bytes2 - nvl(f.bytes, 0), 0) / 1024 / 1024, '99999999.99') "Used (M)",
       to_char(nvl(nvl(f.bytes, 0), 0) / 1024 / 1024, '99999999.99') "Free (M)",
       to_char(nvl((a.bytes2 - nvl(f.bytes, 0)) / a.bytes2 * 100, 0),'990.99') "Used %",
       to_char(nvl((a.bytes2 - nvl(f.bytes, 0)) / a.bytes * 100, 0),'990.99') "Ext_Used %"
  FROM sys.dba_tablespaces d,  (SELECT tablespace_name, SUM(greatest(BYTEs,MAXBYTES)) bytes,SUM(BYTES) bytes2 FROM dba_data_files GROUP BY tablespace_name) a,       (SELECT tablespace_name, SUM(BYTES) bytes FROM dba_free_space GROUP BY tablespace_name) f WHERE d.tablespace_name = a.tablespace_name(+)    AND d.tablespace_name = f.tablespace_name(+) order by 8,7 '''
sql_tps='''
select name,stat_val from v$SYSSTAT where name in ('transaction total count')'''
sql_base='''select * from (
select name, arch_mode,last_startup_time from v$database) ,
(select para_value from v$dm_ini where para_name='PORT_NUM'), 
(select  product_type from v$license)'''
# sql_port='''
# select para_value from v$dm_ini where para_name='PORT_NUM'''
# sql_version='''
# select * from v$version where rownum=1'''




##直接展现结果值
def get_base_stat():
    dm_conn = dmPython.connect(user=user_name, password=passwd, server=server_name, port=server_port)
    dm_cursor = dm_conn.cursor()
    try:
        dm_cursor.execute(sql_base)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    aa = dm_cursor.fetchone()


    dm_cursor.close()
    dm_conn.close()
    print (aa)
    dblastuptime=aa[2]
    print(dblastuptime)
    #base = Info('dmdb_version', 'Description of info')
    #base.info({'数据库名': aa[0], '数据库版本': aa[4], '数据库端口': aa[3], '归档模式': aa[1], 'DBINFO': dbtype})
    # dbname.labels(dbname=i[0],DBINFO=dbtype)
    # db_arch_mode.labels(archmode=i[1],DBINFO=dbtype)
    # db_uptime.labels(uptime=i[2],DBINFO=dbtype)

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "***get_base_stat is done")



def get_tps_stat():
    dm_conn = dmPython.connect(user=user_name, password=passwd, server=server_name, port=server_port)
    dm_cursor = dm_conn.cursor()
    try:
        dm_cursor.execute(sql_tps)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    aa = dict(dm_cursor.fetchall())

    #定时时间间隔2秒，取差值
    time.sleep(2)

    try:
        dm_cursor.execute(sql_tps)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    bb = dict(dm_cursor.fetchall())

    dm_cursor.close()
    dm_conn.close()

    #print ('aa-1:',aa)
    #print ('bb-2:',bb)

    # 遍历字典中的每一个key
    for key in bb.keys():
        #print(bb[key]-aa[key])
        tpsstat.labels(type=key,DBINFO=dbtype).set(bb[key]-aa[key])
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "***get_tps_stat is done")


##直接展现结果值
def get_session_stat():
    dm_conn = dmPython.connect(user=user_name, password=passwd, server=server_name, port=server_port)
    dm_cursor = dm_conn.cursor()
    try:
        dm_cursor.execute(sql_session)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    aa = dm_cursor.fetchall()
    dm_cursor.close()
    dm_conn.close()
    #print (aa)
    for i in aa:
        #print (i[0])
        #print(i[1])
        sessionstat.labels(type=i[0],DBINFO=dbtype).set(i[1])
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"***get_session_stat is done")

    #g.labels(hostip=host_ip).set(cup_use_percent)  # 本机IP传入labels，CPU使用率传入value



##取2次查询差值
def get_dml_stat():
    dm_conn = dmPython.connect(user=user_name, password=passwd, server=server_name, port=server_port)
    dm_cursor = dm_conn.cursor()
    try:
        dm_cursor.execute(sql_dml)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    aa = dict(dm_cursor.fetchall())

    #定时时间间隔2秒，取差值
    time.sleep(2)

    try:
        dm_cursor.execute(sql_dml)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    bb = dict(dm_cursor.fetchall())

    dm_cursor.close()
    dm_conn.close()

    #print ('aa-1:',aa)
    #print ('bb-2:',bb)

    # 遍历字典中的每一个key
    for key in bb.keys():
        #print(bb[key]-aa[key])
        dmlstat.labels(type=key,DBINFO=dbtype).set(bb[key]-aa[key])
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "***get_dml_stat is done")


def get_load_stat():
    dm_conn = dmPython.connect(user=user_name, password=passwd, server=server_name, port=server_port)
    dm_cursor = dm_conn.cursor()
    try:
        dm_cursor.execute(sql_load)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    aa = dict(dm_cursor.fetchall())

    #定时时间间隔2秒，取差值
    time.sleep(2)

    try:
        dm_cursor.execute(sql_load)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    bb = dict(dm_cursor.fetchall())

    dm_cursor.close()
    dm_conn.close()

    if debug:
        print ('aa-1:',aa)
        print ('bb-2:',bb)

    # 遍历字典中的每一个key
    for key in bb.keys():
        if debug:
            print(key)
        #print(bb[key]-aa[key])
        loadstat.labels(type=key,DBINFO=dbtype).set(bb[key]-aa[key])
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), "***get_load_stat is done")

    #g.labels(hostip=host_ip).set(cup_use_percent)  # 本机IP传入labels，CPU使用率传入value


def get_tbs_stat():
    dm_conn = dmPython.connect(user=user_name, password=passwd, server=server_name, port=server_port)
    dm_cursor = dm_conn.cursor()
    try:
        dm_cursor.execute(sql_tbs)
    except Exception as e:
        print(' dm_cursor.execute(...) Error: ', e)
    aa = dm_cursor.fetchall()
    dm_cursor.close()
    dm_conn.close()

    #print (aa)
    for i in aa:
        # print(i[0])
        # print(i[1])
        # print(i)

        tbs.labels(i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],dbtype)

    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),"***get_tbs_stat is done")




if __name__ == '__main__':
    start_http_server(8000)  # 8000端口启动
    print(dmdb_exporter_copyrights)
    while True:
        get_session_stat()
        get_dml_stat()
        get_load_stat()
        get_tps_stat()

        get_tbs_stat()
        #get_base_stat()
        print('******************work done******************')

        #自定义性能指标采集循环周期，默认5秒
        time.sleep(5)