from numpy import array
from string import punctuation

import json
from nltk.corpus import stopwords

import luigi
from luigi.format import UTF8


## =========  This program shows a simple Luigi pipeline ============
## The task load_tokens gets data from a file and writes it
## in proper format for the next task
## The task process_tokens takes the output of first task as input
## and does some processing like removing punctuations, stopwords
## and single alphabets, and finally writes the processed tokens
## to an output file

class load_tokens(luigi.Task):
    
    def requires(self):
        return []
    
    def output(self):
        return luigi.LocalTarget("review_tokens.txt",format=UTF8)
    
    def run(self):
        with open('\\path_to_file\\reviews.json',encoding='utf-8') as f:
            data = json.load(f)
        tokens = []
        for i in range(len(data)):
            token = data[i][0].split()
            tokens.append(token)
        
        with self.output().open('w') as f:
            for i in range(len(data)):
                f.write("{}\n".format(tokens[i]))


class process_tokens(luigi.Task):

    
    def requires(self):
        return [load_tokens()]

    def output(self):
        return luigi.LocalTarget("processed_tokens.txt", format=UTF8)

    def run(self):
        table = str.maketrans('','', punctuation)
        stop_words = set(stopwords.words('english'))
        with self.input()[0].open() as fin, self.output().open('w') as fout:
            for line in fin:
                tokens = list(line.split(","))
                tokens = [word.strip() for word in tokens]
                tokens = [word.translate(table) for word in tokens]
                tokens = [word for word in tokens if word.isalpha()]
                tokens = [word for word in tokens if not word in stop_words]
                tokens = [word for word in tokens if len(word) > 1]
                fout.write("{}\n".format(tokens))
                


if __name__ == '__main__':
    luigi.run()
