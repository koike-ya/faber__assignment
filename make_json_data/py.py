import os
import urllib
import tarfile
from collections import defaultdict
import json

# print(items['label_names'])

def save_file(url, save_path):
    file_name = url.split('/')[-1]
    file_path = os.path.join(save_path, file_name)
    if os.path.exists(file_path):
        print('{} already exists.'.format(file_path))
    else:
        print('Downloading {}'.format(file_name))
        file_path, _ = urllib.urlretrieve(url, file_path)
    return file_path
    
def extract_file(file_path, save_path):
    with tarfile.open(file_path, 'r') as tf:
        tf.extractall(save_path)

def make_corpus(data_dir):
    corpus = {'data': [], 'label': [], 'label_names': []}
    vocabulary = defaultdict()
    vocabulary.default_factory = vocabulary.__len__
    
    for file_or_dir in os.listdir(data_dir):
        if file_or_dir.endswith('.txt'):
            continue
        label = file_or_dir
        for file in os.listdir(os.path.join(data_dir, label)):
            if file == 'LICENSE.txt':
                continue
            with open(os.path.join(os.path.join(data_dir, label, file))) as f:
                text = f.read()
                corpus['data'].append(text)
                corpus['label'].append(vocabulary[label])
    corpus['label_names'] = dict((v,k) for k,v in vocabulary.items())
    return corpus

def save_corpus(processed_dir, corpus):
    with open(os.path.join(processed_dir, 'livedoor.json'), 'w') as f:
        json.dump(corpus, f)

def main(project_dir, save_path):
    url = 'http://www.rondhuit.com/download/ldcc-20140209.tar.gz'
    file_path = save_file(url=url, save_path=save_path)
    
    data_dir = os.path.join(raw_dir, 'text')
    if not os.path.exists(data_dir):
        extract_file(file_path, save_path)
    
    corpus = make_corpus(data_dir)
    processed_dir = os.path.join(project_dir, 'data/processed')
    if not os.path.exists(os.path.join(processed_dir, 'livedoor.json')):
        save_corpus(processed_dir, corpus)
    
if __name__ == '__main__':
    project_dir = os.path.join(os.path.dirname(__file__))
    raw_dir = os.path.join(project_dir, 'data/raw')
    
    main(project_dir, raw_dir)