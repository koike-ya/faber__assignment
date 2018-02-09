import json
import os
import re
import MeCab
import numpy as np
import keras
from keras.utils import np_utils
from keras.models import Sequential, model_from_json
from keras.layers import Dense
from keras.preprocessing import sequence
import collections

# import gensim.parsing.preprocessing
from gensim import corpora, matutils
from sklearn.model_selection import train_test_split

DIC_NAME = 'dic_raw_full4.txt'

def load_json(data_dir):
    with open(os.path.join(data_dir, 'livedoor.json')) as f:
        items = json.load(f)
    return items

def clean_text(text):
    replaced_text = '\n'.join(s.strip() for s in text.splitlines()[2:] if s != '')  # skip header by [2:]
    replaced_text = replaced_text.lower()
    replaced_text = re.sub(r'[【】]', ' ', replaced_text)       # 【】の除去
    replaced_text = re.sub(r'[（）()]', ' ', replaced_text)     # （）の除去
    replaced_text = re.sub(r'[［］\[\]]', ' ', replaced_text)   # ［］の除去
    replaced_text = re.sub(r'[@＠]\w+', '', replaced_text)  # メンションの除去
    replaced_text = re.sub(r'https?:\/\/.*?[\r\n ]', '', replaced_text)  # URLの除去
    replaced_text = re.sub(r'　', ' ', replaced_text)  # 全角空白の除去
    return replaced_text

def tokenize(text):
    mecabTagger = MeCab.Tagger('mecal-ipadic-neologd')
    word_list = []
    res = mecabTagger.parseToNode(text)
    while res:
        pos = res.feature.split(",")
        if pos[0] in ["名詞"]:
            if not pos[1] in ["代名詞", "固有名詞", "数", "非自立", "特殊"]:
                try:
                    word_list.append(res.surface)
                except UnicodeDecodeError:
                    print('デコードエラー→'+pos[0]+pos[1]+pos[2])
        res = res.next
    return word_list

def make_words_list(data):
    words_list = [clean_text(text) for text in data]
    words_list = [tokenize(text) for text in words_list]
    return words_list

def load_dic(project_dir, words_list):
    DIC_DIR = os.path.join(project_dir, 'dic', DIC_NAME)
    if not os.path.exists(DIC_DIR):
        dictionary = corpora.Dictionary(words_list)
        dictionary.filter_extremes(no_below=2, no_above=0.8)
        dictionary.save_as_text(DIC_DIR)
    dic = corpora.Dictionary.load_from_text(DIC_DIR)
    return dic

def make_data_set(words_list, dic):
    # 辞書の次元→ len(dic.keys()) or len(dic.values())
    vecs = [dic.doc2bow(word_list) for word_list in words_list]
    x = [matutils.corpus2dense([vec], num_terms=len(dic)).T[0] for vec in vecs]
    y = items['label']
    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=0.8)
    x_train = np.array(x_train)
    x_test = np.array(x_test)
    y_test = np.array(y_test)
    y_train = np.array(y_train)
    x_train = sequence.pad_sequences(x_train, maxlen=len(x_train[0]))
    x_test = sequence.pad_sequences(x_test, maxlen=len(x_train[0]))
    y_train = np_utils.to_categorical(y_train)
    y_test = np_utils.to_categorical(y_test)
    return x_train, x_test, y_train, y_test, x

def make_model(input_dim, output_dim):
    # set parameters:
    first_hidden=400
    second_hidden=200
    third_hidden=100
    fourth_hidden=50

    # print('Build model...')
    model = Sequential()
    model.add(Dense(first_hidden, input_dim=input_dim,  activation="relu"))
    model.add(Dense(second_hidden, input_dim=first_hidden,  activation="relu"))
    model.add(Dense(third_hidden, input_dim=second_hidden,  activation="relu"))
    model.add(Dense(fourth_hidden, input_dim=third_hidden,  activation="relu"))
    model.add(Dense(output_dim, input_dim=fourth_hidden,  activation="softmax"))
    return model

def load_model(input_dim, output_dim):
    model_path = os.path.join(project_dir, 'model/model_json1.json')
    if not os.path.exists(model_path):
        print('made {0}'.format(model_path))
        model = make_model(input_dim, output_dim)
        with open(model_path, 'w') as f:
            json.dump(model.to_json(), f)
    else:
        with open(model_path, 'r') as f:
            try:
                json_string = json.load(f)
                model = model_from_json(json_string)
                print('loaded from {0}'.format(model_path))
            except UnicodeDecodeError:
                model = make_model(input_dim, output_dim)
                print('made {0}/{1}.'.format(model_path.split('/')[-2], model_path.split('/')[-1]))
    return model, model_path



if __name__ == '__main__':
    project_dir = os.path.dirname(__file__)
    DATA_DIR = os.path.join(project_dir, 'data/processed')
    items = load_json(DATA_DIR)
    words_list = make_words_list(items['data'])
    dic = load_dic(project_dir, words_list) # ストップワードの除去で精度上がるかも。
    
    x_train, x_test, y_train, y_test, x = make_data_set(words_list, dic)
    model, model_path = load_model(input_dim=len(x_train[0]), output_dim=len(y_train[0]))
    model.summary()
    earlystopping = keras.callbacks.EarlyStopping(monitor='acc', verbose=1, patience=5, mode='auto')
    model_checkpoint = keras.callbacks.ModelCheckpoint(model_path, monitor='acc', save_best_only=True, mode='auto', period=1)
    model.compile(loss="categorical_crossentropy", optimizer="rmsprop", metrics=["accuracy"])
    model.fit(x_train, y_train, epochs=200, batch_size=128, callbacks=[earlystopping, model_checkpoint], verbose=0)
    print("Now learning from data...")
    
    scores = model.evaluate(x_test, y_test)
    print("%s: %.2f%%" % (model.metrics_names[1], scores[1] * 100))
    classes = model.predict_classes(x_test, batch_size=128)
    proba = model.predict_proba(x_test, batch_size=128)
    
    # 各ラベルの項目数をカウント
    count_list = collections.Counter(classes)
    for k in sorted(count_list.keys()):
        print("category {0} ({1}) has {2} items".format(k, items['label_names'][str(k)], count_list[k]))
    
    # 各ラベルにとって典型的なデータをそれぞれ表示
    counter1 = range(len(y_test[0]))
    counter2 = range(len(x[0]))
    typical_list = [(np.argmax(proba[:,j])) for j in range(len(y_test[0]))]
    print(typical_list)
    for index, count1 in zip(typical_list, counter1):
        print("Most typical content in category {0} ({1}) is this below".format(count1, items['label_names'][str(count1)]))
        for each_x, count2 in zip(x, counter2):
            if np.allclose(x_test[index], each_x):
                print(items['data'][count2])
                break
    
    # 間違ったラベルへの分類をしたデータを確認
    for i in range(len(y_test)):
        itemindex = np.where(y_test[i] == 1)
        if classes[i] != itemindex[0]:
            print("count {0}  ==>  wrong predict : {1} , answer is {2}".format(i, classes[i], itemindex[0][0]))