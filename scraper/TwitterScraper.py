# -*- coding: utf-8 -*-
import os
from requests_oauthlib import OAuth1Session
import json
import yaml
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
    def bySearch(keyword, CS, CK, AT, AS):
        return TweetsGetterBySearch(keyword, CS, CK, AT, AS)

    @staticmethod
    def byUser(screen_name, since_id, CS, CK, AT, AS):
        return TweetsGetterByUser(screen_name, since_id, CS, CK, AT, AS)

class TweetsGetterBySearch(TweetsGetter):
    '''
    キーワードでツイートを検索
    '''

    def __init__(self, keyword, CS, CK, AT, AS):
        super(TweetsGetterBySearch, self).__init__(CS, CK, AT, AS)
        self.keyword = keyword

    def specifyUrlAndParams(self):
        '''
        呼出し先 URL、パラメータを返す
        '''
        url = 'https://api.twitter.com/1.1/search/tweets.json'
        params = {'q': self.keyword, 'count': 100}
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


if __name__ == '__main__':

    app_yml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config', 'twitter_app.yml'))
    
    with open(app_yml_path) as f:
        app_config = yaml.safe_load(f.read())

    user = 0
    CS = app_config[user]['consumer_secret']
    CK = app_config[user]['consumer_key']
    AT = app_config[user]['access_token']
    AS = app_config[user]['access_token_secret']
    
    input_yml_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config', 'input.yml'))

    with open(input_yml_path) as f:
        inputs = yaml.safe_load(f.read())

    # コネクション作成
    client = MongoClient('localhost', 27017)
    
#     byUserの検索
    byUser = 1
    for each_name in range(len(inputs[byUser])):
        # コネクションから特定ユーザ用のデータベースを取得
        db = client['by_user_database']
        # データベースからコレクションを取得
        collection = db[inputs[byUser][each_name]['collection_name']]
        
#         tweetのIDの最大値を取得
        doc = list(collection.find({}, {'_id': False, 'id': True}).sort("id", -1).limit(1))
        
    #   TODO DBが初期化されている場合、最初から取ってくると死んじゃうので適当な所から。
        if len(doc) == 0:
            since_id = 700000000000000000
    #   DBにデータがある場合
        if len(doc) != 0:
            since_id = doc[0]['id']
        
        # ジェネレータから次々取ってきてDBに保存
        getter = TweetsGetter.byUser(inputs[byUser][each_name]['screen_name'], since_id, CS, CK, AT, AS)
        all_tweets = getter.collect()
        for tweet in list(all_tweets):
            collection.insert_one(tweet)
        
        # 取ってきたtweetのユーザ名と次回取ってくるidを取得
        doc_after = list(collection.find({}).sort('id', -1).limit(1))
        if len(doc_after) != 0:    
            print('name:', doc_after[0]['user']['name'])
            next_since_id = doc_after[0]['id']
            print('next_since_id:',next_since_id)
        

