'''
Created on Dec 2, 2017

@author: koiketomoya
'''
# -*- coding: utf-8 -*-

from requests_oauthlib import OAuth1Session
import json
import yaml
import csv
import datetime, time, sys
from abc import ABCMeta, abstractmethod
from pymongo import MongoClient



class TweetsGetter(object):
    __metaclass__ = ABCMeta

    def __init__(self, CS, CK, AT, AS):
        self.session = OAuth1Session(CK, CS, AT, AS)

    @abstractmethod
    def specifyUrlAndParams(self, keyword):
        '''
        呼出し先 URL、パラメータを返す
        '''

    @abstractmethod
    def pickupTweet(self, res_text, includeRetweet):
        '''
        res_text からツイートを取り出し、配列にセットして返却
        '''

    @abstractmethod
    def getLimitContext(self, res_text):
        '''
        回数制限の情報を取得 （起動時）
        '''

    def collect(self, total=-1, onlyText=False, includeRetweet=False):
        '''
        ツイート取得を開始する
        '''

        # ----------------
        # 回数制限を確認
        # ----------------
        self.checkLimit()

        # ----------------
        # URL、パラメータ
        # ----------------
        url, params = self.specifyUrlAndParams()
        params['include_rts'] = str(includeRetweet).lower()
        # include_rts は statuses/user_timeline のパラメータ。search/tweets には無効

        # ----------------
        # ツイート取得
        # ----------------
        cnt = 0
        unavailableCnt = 0
        while True:
            res = self.session.get(url, params=params)
            if res.status_code == 503:
                # 503 : Service Unavailable
                if unavailableCnt > 10:
                    raise Exception('Twitter API error %d' % res.status_code)

                unavailableCnt += 1
                print ('Service Unavailable 503')
                self.waitUntilReset(time.mktime(datetime.datetime.now().timetuple()) + 30)
                continue

            unavailableCnt = 0

            if res.status_code != 200:
                raise Exception('Twitter API error %d' % res.status_code)
            
            # followerのリストをゲットする時
#             if 'users' in json.loads(res.text).keys():
            if 'users' in json.loads(res.text):
                follower_data = json.loads(res.text)
                yield follower_data
                return
            else:
                tweets = self.pickupTweet(json.loads(res.text))
            
            if len(tweets) == 0:
                # len(tweets) != params['count'] としたいが
                # count は最大値らしいので判定に使えない。
                # ⇒  "== 0" にする
                # https://dev.twitter.com/discussions/7513
                break
                

            for tweet in tweets:
                if (('retweeted_status' in tweet) and (includeRetweet is False)):
                    pass
                else:
                    if onlyText is True:
                        yield tweet['text']
                    else:
                        yield tweet

                    cnt += 1
                    if cnt % 100 == 0:
                        print ('%d件 ' % cnt)

                    if total > 0 and cnt >= total:
                        return
            if 'id' in tweet.keys():
                params['max_id'] = tweet['id'] - 1

            # ヘッダ確認 （回数制限）
            # X-Rate-Limit-Remaining が入ってないことが稀にあるのでチェック
            if ('X-Rate-Limit-Remaining' in res.headers and 'X-Rate-Limit-Reset' in res.headers):
                if (int(res.headers['X-Rate-Limit-Remaining']) == 0):
                    self.waitUntilReset(int(res.headers['X-Rate-Limit-Reset']))
                    self.checkLimit()
            else:
                print ('not found  -  X-Rate-Limit-Remaining or X-Rate-Limit-Reset')
                self.checkLimit()

    def checkLimit(self):
        '''
        回数制限を問合せ、アクセス可能になるまで wait する
        '''
        unavailableCnt = 0
        while True:
            url = "https://api.twitter.com/1.1/application/rate_limit_status.json"
            res = self.session.get(url)
            
            if res.status_code == 503:
                # 503 : Service Unavailable. Twitter is now busy. Try later
                if unavailableCnt > 10:
                    raise Exception('Twitter API error %d' % res.status_code)

                unavailableCnt += 1
                print ('Service Unavailable 503')
                self.waitUntilReset(time.mktime(datetime.datetime.now().timetuple()) + 30)
                continue

            unavailableCnt = 0

            if res.status_code != 200:
                raise Exception('Twitter API error %d' % res.status_code)

            remaining, reset = self.getLimitContext(json.loads(res.text))
            if (remaining == 0):
                self.waitUntilReset(reset)
            else:
                break

    def waitUntilReset(self, reset):
        '''
        reset 時刻まで sleep
        '''
        seconds = reset - time.mktime(datetime.datetime.now().timetuple())
        seconds = max(seconds, 0)
        print ('\n     =====================')
        print ('     == waiting %d sec ==' % seconds)
        print ('     =====================')
        sys.stdout.flush()
        time.sleep(seconds + 10)  # 念のため + 10 秒

    @staticmethod
    def bySearch(keyword, since_id, CS, CK, AT, AS):
        return TweetsGetterBySearch(keyword, since_id, CS, CK, AT, AS)

    @staticmethod
    def byUser(screen_name, since_id, CS, CK, AT, AS):
        return TweetsGetterByUser(screen_name, since_id, CS, CK, AT, AS)
    
    @staticmethod
    def byFollower(screen_name, count, CS, CK, AT, AS):
        return TweetsGetterByFollower(screen_name, count, CS, CK, AT, AS)


class TweetsGetterBySearch(TweetsGetter):
    '''
    キーワードでツイートを検索
    '''

    def __init__(self, keyword, since_id, CS, CK, AT, AS):
        super(TweetsGetterBySearch, self).__init__(CS, CK, AT, AS)
        self.keyword = keyword
        self.since_id = since_id
        self.result_type = 'recent'
        # mixed, recentの順に選ぶ

    def specifyUrlAndParams(self):
        '''
        呼出し先 URL、パラメータを返す
        '''
        url = 'https://api.twitter.com/1.1/search/tweets.json'
        params = {'q': self.keyword, 'since_id': self.since_id, 'result_type': self.result_type, 'count': 100}
        return url, params

    def pickupTweet(self, res_text):
        '''
        res_text からツイートを取り出し、配列にセットして返却
        '''
        results = []
        for tweet in res_text['statuses']:
            results.append(tweet)

        return results

    def getLimitContext(self, res_text):
        '''
        回数制限の情報を取得 （起動時）
        '''
        remaining = res_text['resources']['search']['/search/tweets']['remaining']
        reset = res_text['resources']['search']['/search/tweets']['reset']

        return int(remaining), int(reset)


class TweetsGetterByUser(TweetsGetter):
    '''
    ユーザーを指定してツイートを取得
    '''

    def __init__(self, screen_name, since_id, CS, CK, AT, AS):
        super(TweetsGetterByUser, self).__init__(CS, CK, AT, AS)
        self.screen_name = screen_name
        self.since_id = since_id

    def specifyUrlAndParams(self):
        '''
        呼出し先 URL、パラメータを返す
        '''
        url = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
        params = {'screen_name': self.screen_name, 'count': 200, 'since_id': self.since_id}
        return url, params

    def pickupTweet(self, res_text):
        '''
        res_text からツイートを取り出し、配列にセットして返却
        '''
        results = []
        for tweet in res_text:
            results.append(tweet)

        return results

    def getLimitContext(self, res_text):
        '''
        回数制限の情報を取得 （起動時）
        '''
        remaining = res_text['resources']['statuses']['/statuses/user_timeline']['remaining']
        reset = res_text['resources']['statuses']['/statuses/user_timeline']['reset']
        return int(remaining), int(reset)


class TweetsGetterByFollower(TweetsGetter):
    '''
    ユーザーを指定して、フォロワーのツイートを取得
    '''
    
    def __init__(self, screen_name, count, CS, CK, AT, AS):
        super(TweetsGetterByFollower, self).__init__(CS, CK, AT, AS)
        self.screen_name = screen_name
        self.count = count
    
    def specifyUrlAndParams(self):
        '''
        呼出し先 URL、パラメータを返す
        '''
        url = 'https://api.twitter.com/1.1/followers/list.json'
        params = {'screen_name': self.screen_name, 'count': self.count}
        return url, params
    
    def pickupTweet(self, res_text):
        '''
        res_text からツイートを取り出し、配列にセットして返却
        '''
        return res_text
    
    def getLimitContext(self, res_text):
        
        remaining = res_text['resources']['followers']['/followers/list']['remaining']
        reset = res_text['resources']['followers']['/followers/list']['reset']
        return int(remaining), int(reset)
    

#     mongoDBとの接続
class SaveTweers(object):
    def __init__(self, dbName, keyword):
        # コネクション作成
        self.client = MongoClient('localhost', 27017)
        self.dbName = dbName
        # コネクションから特定ユーザ用のデータベースを取得
        self.db = self.client[self.dbName]
        self.keyword = keyword
    
    @abstractmethod
    def getCollection(self):
        '''
        コレクション名を返す
        '''
    
    @abstractmethod
    def getGetter(self, CS, CK, AT, AS):
        '''
        ジェネレータを取得
        '''
    
    @abstractmethod
    def insert(self, tweet):
        '''
        得られたtweetを加工してDBに保存
        '''
    
    @abstractmethod
    def printResults(self):
        '''
        結果を表示
        '''
        
    def save(self, CS, CK, AT, AS):
        collection = self.getCollection()
        # tweetのIDの最大値を取得
        doc = list(collection.find({}, {'_id': False, 'id': True}).sort("id", -1).limit(1))
        
    #   TODO DBが初期化されている場合、最初から取ってくると死んじゃうので適当な所から。
        if len(doc) == 0:
            since_id = 700000000000000000
    #   DBにデータがある場合
        if len(doc) != 0:
            since_id = doc[0]['id']
        
        # ジェネレータを作成
        getter = self.getGetter(since_id, CS, CK, AT, AS)
        all_tweets_list = list(getter.collect())
        fileName = self.hashtag+".csv"
        with open(fileName, 'w', newline = '', encoding='utf-8') as csvFile:
            csvwriter = csv.writer(csvFile, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            for i, tweet in enumerate(all_tweets_list):
                if i == 0:
                    keylist = self.insert(tweet)
                    keylist.insert(0, "user_page")
                    csvwriter.writerow(keylist)
                valuelist = []
                user_page = 'https://twitter.com/'+tweet['user']['screen_name']
                for key in keylist:
                    if key == 'user_page':
                        valuelist.append(user_page)
                    elif key in tweet['user']:
                        valuelist.append(str(tweet['user'][key]))
                    else:
                        print(key)
                        valuelist.append('null')
                        
                csvwriter.writerow(valuelist)
        
    @staticmethod
    def byUser(dbName, userName, keyword):
        return SaveTweetsByUser(dbName, userName, keyword)
    
    @staticmethod
    def byUserlist(dbName, keyword):
        return SaveTweersByUserlist(dbName, keyword)
        
#     byUserでの検索・保存
class SaveTweetsByUser(SaveTweers):
    def __init__(self, dbName, userName, keyword):
        super(SaveTweetsByUser, self).__init__(dbName, keyword)
        self.userName = userName
    
    def getCollection(self):
        self.collection = self.db[self.userName]
        return self.collection
        
    def getGetter(self, since_id, CS, CK, AT, AS):
        getter = TweetsGetter.byUser(self.userName, since_id, CS, CK, AT, AS)
        return getter
    
    def insert(self, tweet):
        self.collection.insert_one(tweet)
        
    def printResults(self):
        # データが取れているか確認
        doc_after = list(self.collection.find({}).sort("id", -1).limit(1))
        print("name:", doc_after[0]['user']['name'])
        print("screen_name:", doc_after[0]['user']['screen_name'])
        # 検索をかけてcount()で件数を取ってくる。
        print("count:", self.collection.find( {"entities.hashtags.text":self.keyword}).count())
        
class SaveTweersByUserlist(SaveTweers):
    def __init__(self, dbName, hashtag):
        super(SaveTweersByUserlist, self).__init__(dbName, hashtag)
        self.hashtag = hashtag
        self.csvData = ""
        
    def getCollection(self):
        self.collection = self.db['userList']
        return self.collection
    
    def getGetter(self, since_id, CS, CK, AT, AS):
        getter = TweetsGetterBySearch(self.hashtag, since_id, CS, CK, AT, AS)
        return getter
    
    def insert(self, tweet):
        # tweetについているユーザに関する情報を入れる
        self.keylist = list(tweet['user'].keys())
        return self.keylist
                            
    def printResults(self):
        return
    
# 辞書型からCSVへの変換をクラス化したい。
# class convertDictToCSV(object):
#     def __init__(self):
#         return
#     
#     def makeHeader(self):
#         csvData = ""
#         for i, key in enumerate(keylist):
#             if i < len(keylist) - 1:
#                 csvData = csvData+'"'+key+'"'+','
#             elif i == len(keylist) - 1:
#                 csvData = csvData+'"'+key+'"'+'\n'

if __name__ == '__main__':

    app_yml_path = "config/twitter_app.yml"
    app_config = yaml.load(open(app_yml_path))
    
    user = 0
    CS = app_config[user]["consumer_secret"]
    CK = app_config[user]["consumer_key"]
    AT = app_config[user]["access_token"]
    AS = app_config[user]["access_token_secret"]
#     TODO ymlファイルからの日本語入力
#     input_yml_path = open('config/input2.yml').read()
#     input_yml_path = input_yml_path.decode('utf-8')
#     print(type(input_yml_path))
#     inputs = yaml.load(input_yml_path)

#     ymlファイルから日本語がDecodeErrorで取れてないため、一時的に直書き
    inputs = ['bySearch', [{'keyword': '源クラ'}]]
#     inputs = []
        
#     # ユーザー検索
#     for each_name in range(len(inputs[1])):       
#         SaveTweers.byUser(inputs[1][each_name]['database_name'])

    
#     bySearchでの検索・保存
#     TODO DBに入っていない、更新分だけ取ってくる
    bySearch = 1
    # コネクション作成
    client = MongoClient('localhost', 27017)
    db = client['hashtag_search_database']
    for i in range(len(inputs[bySearch])):
        collection = db[inputs[bySearch][i]['keyword']]
        keyword = inputs[bySearch][i]['keyword']
        hashtag = '#' + keyword
        print(hashtag)
        db_name = hashtag
        # DB名にドットが入っていたらアンダーバーに置換
        if "." in db_name:
            db_name = db_name.replace(".", "_")
        
        # ユーザリストを取ってくる場合
        userlist = SaveTweers.byUserlist(hashtag, hashtag)
        userlist.save(CS, CK, AT, AS)
        
        # ユーザのツイートをメインに取ってくる場合
#         since_id = 800000000000000000
#         numTweets = 5
#         # ハッシュタグで検索をかける
#         hashtagGetter = TweetsGetterBySearch(hashtag, since_id, CS, CK, AT, AS)
#         tweetsByHashtag = list(hashtagGetter.collect())
#         for order in range(numTweets):
#             influencer_name = tweetsByHashtag[order]['user']['name']
#             # TODO 有名tweetのユーザが被っていたら次へ
#             # 有名なtweetをしているツイーターのフォロワー数を取っている。変更可。
#             screen_name = tweetsByHashtag[order]['user']['screen_name']
#             # ユーザのtweetを取ってきてそれぞれのコレクションに保存する
#             a = SaveTweers.byUser(db_name, screen_name, keyword)
#             a.save(CS, CK, AT, AS)
            
        
        
# #     byFollowerでの検索・保存
#     count = 2
#     byFollower = 3
#     artistId = 1
#     since_id = 800000000000000000
#     followerList = list(TweetsGetter.byFollower('realDonaldTrump', count, CS, CK, AT, AS).collect())
#     db = client[inputs[byFollower][0]['database_name']]
#     for i in range(count):
#         collection = db[followerList[0]['users'][i]['screen_name']]
#         getter = TweetsGetter.byUser(followerList[0]['users'][i]['screen_name'], since_id, CS, CK, AT, AS)
#         all_tweets = getter.collect()
#      
#         for tweet in list(all_tweets):
#             collection.insert_one(tweet)
#         doc_after = list(collection.find({}).sort("id", -1).limit(1))
#         if len(doc_after) != 0:
#             print(doc_after[0])
#             next_since_id = doc_after[0]['id']
#             print("next_since_id:",next_since_id)
