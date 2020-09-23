#! /bin/bash

source /etc/profile

delete_day2=`date -d -2day '+%Y%m%d'`
delete_day1=`date -d -1day '+%Y%m%d'`

echo delete_day2:$delete_day2
echo delete_day1:$delete_day1

/usr/local/bin/python /home/panniu/hainiu_crawler/download_page/xpath_config.py

# 本地保存最近2天的
rm -rf /home/panniu/xpath_cache_file/xpath_file${delete_day2}*

# hdfs上保存最近1天的
hadoop fs -rmr hdfs://ns1/user/panniu/xpath_cache_file/xpath_file${delete_day1}*