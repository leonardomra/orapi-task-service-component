import en_core_sci_md
import scispacy
import spacy
import io
from sklearn.feature_extraction import text
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation
from tqdm import tqdm
import pandas as pd
import numpy as np
import joblib

class LDA():

    def __init__(self):
        self.nlp = en_core_sci_md.load(disable=['tagger', 'parser', 'ner'])
        self.nlp.max_length = 2000000
        #self.expandStopWords()

    def expandStopWords(self):
        # New stop words list
        self.customize_stop_words = [
            'doi', 'preprint', 'copyright', 'peer', 'reviewed', 'org', 'https', 'et', 'al', 'author', 'figure', 
            'rights', 'reserved', 'permission', 'used', 'using', 'biorxiv', 'fig', 'fig.', 'al.',
            'di', 'la', 'il', 'del', 'le', 'della', 'dei', 'delle', 'una', 'da', 'dell', 'non', 'si',
            'preprint', 'copyright', 'peer-reviewed', 'author/funder', 'doi', 'license', 'biorxiv', 'medrxiv', 'international', 'right', 'display',
            'permission', 'cc-by-nc-nd', 'fig.', 'figure', '=', '°', 'show', 'contain', '<',
            '>', '+', 'al.', '␤', 'ct', '␣', 'ifn-', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
            'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z'
        ]
        # Mark them as stop words
        for w in self.customize_stop_words:
            self.nlp.vocab[w].is_stop = True

    def produceTopics(self, targetTexts, taskParams):
        print('...> Step 1', flush=True)
        vs = self.loadVectors()
        print('...> Step 2', flush=True)
        lda_tf = LatentDirichletAllocation(n_components=taskParams['numberOfTopics'], random_state=0, n_jobs=-1)
        print('...> Step 3', flush=True)
        lda_tf.fit(vs['tf'])
        return {'tf_vectorizer': vs['tf_vectorizer'], 'tf': vs['tf'], 'lda_tf': lda_tf}

    def countWords(self, targetTexts, exists):
        vs = self.vectorize(targetTexts)
        word_count = pd.DataFrame({'word': vs['tf_vectorizer'].get_feature_names(), 'count': np.asarray(vs['tf'].sum(axis=0))[0]})
        word_count = word_count.sort_values('count', ascending=False).set_index('word')[:20].sort_values('count', ascending=True)#.plot(kind='barh')
        return word_count.to_dict()
    
    def vectorize(self, targetTexts):      
        tf_vectorizer = CountVectorizer(tokenizer = self.spacyTokenizer) 
        tf = tf_vectorizer.fit_transform(tqdm(targetTexts.fillna(' ').astype(str)))    
        buffer = io.BytesIO()   
        joblib.dump([tf_vectorizer, tf], buffer, compress=1)
        buffer.seek(0)  
        return buffer

    def loadVectors(self):
        tf_vectorizer, tf = joblib.load('tmp/tml-model.sav')
        tf.shape
        return {'tf_vectorizer': tf_vectorizer, 'tf': tf}

    def dumpVecs(self, tf_vectorizer, tf):
        joblib.dump(tf_vectorizer, 'tf_vectorizer.csv')
        joblib.dump(tf, 'tf.csv')

    def spacyTokenizer(self, sentence):
        return [word.lemma_ for word in self.nlp(sentence) if not (word.like_num or word.is_stop or word.is_punct or word.is_space)] # remove numbers (e.g. from references [1], etc.)
