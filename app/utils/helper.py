import numpy as np


class MeanEmbeddingVectorizer(object):
    def __init__(self, word_model):
        self.word_model = word_model
        self.vector_size = word_model.wv.vector_size

    def fit(self): 
        return self

    def transform(self, sent):  
        sent_word_vector = self.word_average(sent)
        return sent_word_vector

    def word_average(self, sent):
        mean = []
        for word in sent:
            try:
                mean.append(self.word_model.wv.get_vector(word))
            except:
                continue
        if not mean:
            logging.warning("cannot compute average owing to no vector for {}".format(sent))
            return
        else:
            return np.array(mean).mean(axis=0)


