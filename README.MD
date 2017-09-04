README

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