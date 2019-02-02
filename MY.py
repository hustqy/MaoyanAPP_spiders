# -*- coding: utf-8 -*-
# 此程序用来抓取 的数据
import requests
import time
import random
import re
import csv
from multiprocessing.dummy import Pool
from fake_useragent import UserAgent, FakeUserAgentError
from save_data import database

class Spider(object):
	def __init__(self):
		# self.date = '2000-01-01'
		# self.limit = 5000  # 猫眼的评论限制为整个视频的总条数限制
		try:
			self.ua = UserAgent(use_cache_server=False).random
		except FakeUserAgentError:
			pass
		self.db = database()

	def get_headers(self):
		user_agents = ['Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0',
		               'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0) Gecko/20100101 Firefox/18.0',
		               'IBM WebExplorer /v0.94', 'Galaxy/1.0 [en] (Mac OS X 10.5.6; U; en)',
		               'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
		               'Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14',
		               'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; TheWorld)',
		               'Opera/9.52 (Windows NT 5.0; U; en)',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.2pre) Gecko/2008071405 GranParadiso/3.0.2pre',
		               'Mozilla/5.0 (Windows; U; Windows NT 5.2; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.458.0 Safari/534.3',
		               'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.0 (KHTML, like Gecko) Chrome/4.0.211.4 Safari/532.0',
		               'Opera/9.80 (Windows NT 5.1; U; ru) Presto/2.7.39 Version/11.00']
		user_agent = random.choice(user_agents)
		headers = {'Host': 'm.maoyan.com', 'Connection': 'keep-alive',
		           'User-Agent': user_agent,
		           'Referer': 'http://m.maoyan.com/movie/42964/comments?_v_=yes',
		           'Accept': '*/*',
		           'Accept-Encoding': 'gzip, deflate',
		           'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
		           }
		return headers

	def p_time(self, stmp):  # 将时间戳转化为时间
		stmp = float(str(stmp)[:10])
		timeArray = time.localtime(stmp)
		otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
		return otherStyleTime
	
	def replace(self, x):
		# 去除img标签,7位长空格
		removeImg = re.compile('<img.*?>| {7}|')
		# 删除超链接标签
		removeAddr = re.compile('<a.*?>|</a>')
		# 把换行的标签换为\n
		replaceLine = re.compile('<tr>|<div>|</div>|</p>')
		# 将表格制表<td>替换为\t
		replaceTD = re.compile('<td>')
		# 把段落开头换为\n加空两格
		replacePara = re.compile('<p.*?>')
		# 将换行符或双换行符替换为\n
		replaceBR = re.compile('<br><br>|<br>')
		# 将其余标签剔除
		removeExtraTag = re.compile('<.*?>', re.S)
		# 将&#x27;替换成'
		replacex27 = re.compile('&#x27;')
		# 将&gt;替换成>
		replacegt = re.compile('&gt;|&gt')
		# 将&lt;替换成<
		replacelt = re.compile('&lt;|&lt')
		# 将&nbsp换成''
		replacenbsp = re.compile('&nbsp;')
		# 将&#177;换成±
		replace177 = re.compile('&#177;')
		replace1 = re.compile(' {2,}')
		x = re.sub(removeImg, "", x)
		x = re.sub(removeAddr, "", x)
		x = re.sub(replaceLine, "\n", x)
		x = re.sub(replaceTD, "\t", x)
		x = re.sub(replacePara, "", x)
		x = re.sub(replaceBR, "\n", x)
		x = re.sub(removeExtraTag, "", x)
		x = re.sub(replacex27, '\'', x)
		x = re.sub(replacegt, '>', x)
		x = re.sub(replacelt, '<', x)
		x = re.sub(replacenbsp, '', x)
		x = re.sub(replace177, u'±', x)
		x = re.sub(replace1, '', x)
		x = re.sub(re.compile('[\r\n]'), '', x)
		return x.strip()
	
	def GetProxies(self):
		# 代理服务器
		proxyHost = "http-dyn.abuyun.com"
		proxyPort = "9020"
		# 代理隧道验证信息
		proxyUser = "HI18001I69T86X6D"
		proxyPass = "D74721661025B57D"
		proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
			"host": proxyHost,
			"port": proxyPort,
			"user": proxyUser,
			"pass": proxyPass,
		}
		proxies = {
			"http": proxyMeta,
			"https": proxyMeta,
		}
		return proxies
	
	def get_comments_pagenums(self, film_id):  # 获取评论总页数
		url = "http://m.maoyan.com/mmdb/comments/movie/%s.json" % film_id
		querystring = {"_v_": "yes", "offset": '0'}
		retry = 5
		while 1:
			try:
				text = requests.get(url, headers=self.get_headers(), proxies=self.GetProxies(), timeout=10, params=querystring).json()
				comment_num = int(text['total'])
				# if comment_num > self.limit:
				# 	comment_num = self.limit
				if comment_num % 15 == 0:
					pagenums = comment_num / 15
				else:
					pagenums = comment_num / 15 + 1
				return pagenums
			except Exception as e:
				retry -= 1
				if retry == 0:
					print '1',e
					return None
				else:
					continue
	
	def get_comments_page(self, ss):  # 获取每一页的评论
		film_id, product_num, plat_num, page = ss
		print 'page:',page
		url = "http://m.maoyan.com/mmdb/comments/movie/%s.json" % film_id
		querystring = {"_v_": "yes", "offset": str(15 * (page - 1))}
		retry = 5
		while 1:
			try:
				text = requests.get(url, headers=self.get_headers(), proxies=self.GetProxies(),timeout=10, params=querystring).json()
				items = text['cmts']
				results = []
				last_modify_date = self.p_time(time.time())
				for item in items:
					try:
						nick_name = item['nickName']
					except:
						nick_name = ''
					try:
						tmp1 = item['startTime']
						cmt_date = tmp1.split()[0]
						cmt_time = tmp1
						# if cmt_date < self.date:
						# 	continue
					except:
						cmt_date = ''
						cmt_time = ''
					try:
						comments = self.replace(item['content'])
					except:
						comments = ''
					try:
						like_cnt = str(item['approve'])
					except:
						like_cnt = '0'
					try:
						cmt_reply_cnt = str(item['reply'])
					except:
						cmt_reply_cnt = '0'
					long_comment = '0'
					source_url = film_id
					tmp = [product_num, plat_num, nick_name, cmt_date, cmt_time, comments, like_cnt,
					       cmt_reply_cnt, long_comment, last_modify_date, source_url]
					print '|'.join(tmp)
					results.append([x.encode('gbk', 'ignore') for x in tmp])
				return results
			except Exception as e:
				retry -= 1
				if retry == 0:
					print '2',e
					return None
				else:
					continue

	def save_sql(self, table_name,items):  # 保存到sql
		all = len(items)
		print 'all:',all
		results = []
		for i in items:
			try:
				t = [x.decode('gbk', 'ignore') for x in i]
				dict_item = {'product_number': t[0],
				             'plat_number': t[1],
				             'nick_name': t[2],
				             'cmt_date': t[3],
				             'cmt_time': t[4],
				             'comments': t[5],
				             'like_cnt': t[6],
				             'cmt_reply_cnt': t[7],
				             'long_comment': t[8],
				             'last_modify_date': t[9],
				             'src_url': t[10]}
				results.append(dict_item)
			except:
				continue
		for item in results:
			try:
				self.db.add(table_name, item)
			except:
				continue
	
	def get_comments_all(self, film_id, product_number, plat_number):  # 获取总评论数
		pagenums = self.get_comments_pagenums(film_id)
		if pagenums is None:
			print u'网络错误！！！'
			return None
		else:
			print 'pagenums:%d' % pagenums
			ss = [[film_id, product_number, plat_number, page] for page in range(1, pagenums + 1)]
			pool = Pool(10)
			items = pool.map(self.get_comments_page, ss)
			pool.close()
			pool.join()
			mm = []
			for item in items:
				if item is not None:
					mm.extend(item)
			'''
			with open('comment.csv', 'a') as f:
				writer = csv.writer(f, lineterminator='\n')
				writer.writerows(mm)
			'''
			print u'%s 开始录入数据库' % product_number
			self.save_sql('T_COMMENTS_PUB_MOVIE', mm)  # 手动修改需要录入的库的名称
			print u'%s 录入数据库完毕' % product_number


if __name__ == "__main__":
	spider = Spider()
	s = []
	with open('new_data.csv') as f:
		tmp = csv.reader(f)
		for i in tmp:
			if 'ID' not in i[2] and len(i[2]) > 0:
				s.append([i[2], i[0], 'P36'])
	for j in s:
		print j[1],j[0]
		spider.get_comments_all(j[0], j[1], j[2])
	spider.db.db.close()
