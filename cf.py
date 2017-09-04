# -*- coding: utf-8 -*-
"""
Created on Mon Aug 07 15:20:10 2017

@author: ligong

@description:这是用spark来做协同过滤来获得item之间的相似度

说明：
1. 数据输入的说明
数据包含如下的一些字段：
用户id,物品id,对应的值,时间戳
user_id,item_id,value(float or int),timestamp(float or int)

2. 数据输出说明
每一条数据的结构是:
list中有N条
item_id:[(item_id1:similarity_sore),(item_id2:similarity_sore),...]
        
3. 程序运行中的参数说明
    kwargs:运行中的参数
        {
             partition_num:分区数
             line_process:处理输入数据行的函数
             max_num:当用户阅读文章数量大于该值的时候，就不再计算这个用户的点击
             min_num:当item的行为数小于该值的时候，就不再参加计算
             r_len:返回item最相近的若干条
        }
    注：调节partition_num效果比较明显
"""
import time
import math
from operator import add
import pyspark as ps


def collaborative_filtering(sc,input_file,output_file,**kwargs):
    """
    sc:spark content
    input_file:输入文件，支持本地和HDFS文件
    output_file:输出文件，分布式的架构支持HDFS，单机支持本地
    kwargs:运行中的参数
        {
             partition_num:分区数
             line_process:处理输入数据行的函数
             max_num:当用户阅读文章数量大于该值的时候，就不再计算这个用户的点击
             min_num:当item的行为数小于该值的时候，就不再参加计算
             r_len:返回item最相近的若干条
        }
    """
    now = time.time()
    #生成键值对
    def gen_key(line):
        usid,article_id,timestamp,value = line.strip().split(',')
        return ((usid,article_id),float(value)*86400/float(now-int(timestamp)))
    
    #生成对应的值
    def map_value(items):
        #items = kv[1]
        result = []
        for (tid1,v1) in items:
            for (tid2,v2) in items:
                if tid1 >= tid2:
                    continue
                result.append(((tid1,tid2),v1*v2))
        return result
    
    #获得系统中需要用的参数
    partition_num = kwargs.get('partition_num',300)
    line_process = kwargs.get('line_process',gen_key)
    max_num = kwargs.get('max_num',100)
    min_num = kwargs.get('min_num',10)
    r_len = kwargs.get('r_len',20)
    
    
    #生成用户点击的列表，长度超过max_num的过滤掉
    def merge(x,y):
        if x[1] and y[1] and len(x[0]) + len(y[0]) <= max_num:
            return [x[0]+y[0],True]
        return [[],False] 
    
    #找到相似度最高的前r_len个
    def top_n(items):
        items = list(items)
        #items = filter(lambda _:_[1] > 0,items)
        items.sort(key = lambda _:_[1],reverse=True)
        return items[:r_len]

    step_1 = time.time()
    rdd = sc.textFile(input_file,partition_num)
    step_2 = time.time()
    print 'read data using: %s s!' % (step_2-step_1)
    
    #生成用户和item的pair
    user_view_pairs = rdd.map(lambda _:line_process(_)).reduceByKey(add)
    user_view_pairs.cache() 
    step_3 = time.time()
    print 'Gen ((user,item),value) using: %s s!' % (step_3 - step_2)
    
    #生成item的norm
    topic_norm_map = user_view_pairs.map(lambda _:(_[0][1],(_[1]*_[1],1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).filter(lambda _:_[1][1] >= min_num).mapValues(lambda _:math.sqrt(_[0])).collectAsMap()
    #广播
    topic_norm = sc.broadcast(topic_norm_map)
    step_4 = time.time()
    
    print 'Gen (item,normvalue) using: %s s!' % (step_4 - step_3)

    #生成item和item的关联对
    inner_product = user_view_pairs.map(lambda _:(_[0][0],[[(_[0][1],_[1])],True])).reduceByKey(lambda x,y:merge(x,y)).filter(lambda _:_[1][1]).map(lambda _:_[1][0]).flatMap(map_value).reduceByKey(add).repartition(partition_num)
    step_5 = time.time()
    print 'Gen ((item,item),value) using: %s s!' % (step_5 - step_4)

    
    #计算cos的相似度
    def cos(kv):
        #利用广播后的map来join，然后生成cos
        (id1,id2),value = kv[0],kv[1]
        v = topic_norm.value.get(id1,0)*topic_norm.value.get(id2,0)

        if v == 0:
            return []
        cos_value = value/v
        return [(id1,[(id2,cos_value)]),(id2,[(id1,cos_value)])]

    result = inner_product.flatMap(lambda _:cos(_)).reduceByKey(lambda x,y:x+y).mapValues(top_n)
    print result.take(100)
    #result.saveAsTextFile(output_file)
    step_6 = time.time()
    print 'Gen (item_id:[(item_id1:similarity_sore),(item_id2:similarity_sore),...]) using: %s s!' % (step_6 - step_5)

    print 'Total using: %s s!' % (step_6 - step_1)


if __name__ == '__main__':
    conf = ps.SparkConf().setAppName("CF").setMaster("spark://spark31:7077")
    conf.set("spark.executor.memory", "10g")
    conf.set("spark.cores.max","100") 
    conf.set("spark.driver.maxResultSize","5g")
    sc = ps.SparkContext(conf=conf)
    
    #获得需要读取的文件
    now = int(time.time())
    files = []
    now = int(time.time()) 
    days = 9
    for i in xrange(2,days+1):
        start = now - now% 86400 - 3600*8 - i*86400
        fname = 'hdfs:/data/CF/%s.txt' % time.strftime('%Y%m%d',time.localtime(start))
        files.append(fname)
    print 'Using files:',files
    
    input_files = ','.join(files)
    output_files = 'hdfs:/data/CF_result/tmp'
    
    collaborative_filtering(sc,input_files,output_files)
