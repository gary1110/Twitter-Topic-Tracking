# coding:utf-8
import sys

import numpy
import pandas as pd
from sqlalchemy import create_engine
import pymysql
import re
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize
from nltk.tokenize import MWETokenizer
from nltk.stem.porter import PorterStemmer
from sklearn.feature_extraction import DictVectorizer
import copy

def pre_processing(list):
    # Import the package used to delete stop words and stemming
    porter_stemmer = PorterStemmer()
    lemmatizer = WordNetLemmatizer()
    sr = stopwords.words('english')
    sr_append = ["rt", "http", "com", ]
    # Use re expression to delete web pages
    results = re.compile(r'[http|https]*://[a-zA-Z0-9.?/&=:]*', re.S)
    # Here is the place to delete stop words, do lemmatize, do stem, do word segmentation,
    # word_tokenize is word segmentation

    sentences = list.lower()
    grammar = "NP: {<DT>?<JJ>*<NN>|<NNP>*}"
    cp = nltk.RegexpParser(grammar)
    words = word_tokenize(sentences)
    sentence = nltk.pos_tag(word_tokenize(sentences))
    tree = cp.parse(sentence)
    #print
    #"\nNoun phrases:"
    list_of_noun_phrases = extract_phrases(tree, 'NP')
    for phrase in list_of_noun_phrases:
        word = "_".join([x[0] for x in phrase.leaves()])
        if word not in words:
            words.append(word)
    #print(words)
    test_temp = []
    for z in words:
        # filter web link
        z = re.sub(results, '', z)
        # alphabet characters only
        z = re.sub('[^A-Za-z0-9_]+', '', z)
        z = lemmatizer.lemmatize(z)
        # z = porter_stemmer.stem(z)
        # filter stopwords
        if z in sr:
            continue
        if z == '':
            continue
        if z in sr_append:
            continue
        test_temp.append(z)
    # print("After pre-process : ")
    # print(test_temp)
    return test_temp

def extract_phrases(my_tree, phrase):
   my_phrases = []
   if my_tree.label() == phrase:
      my_phrases.append(my_tree.copy(True))
   for child in my_tree:
       if type(child) is nltk.Tree:
            list_of_phrases = extract_phrases(child, phrase)
            if len(list_of_phrases) > 0:
                my_phrases.extend(list_of_phrases)
   return my_phrases


if __name__ == '__main__':
    nltk.download('stopwords')
    nltk.download('punkt')
    nltk.download('wordnet')
    nltk.download('averaged_perceptron_tagger')
    numpy.set_printoptions(threshold=sys.maxsize)
    #Show all columns
    pd.set_option('display.max_columns', None)
    #显示所有行
    pd.set_option('display.max_rows', None)
    #Set the display length of value to 100, the default is 50
    pd.set_option('max_colwidth',30)

    con_engine = create_engine('mysql+pymysql://root:@localhost/499db2?charset=utf8')
    # Database settings
    sql_ = 'select * from zctweets;'
    df_data = pd.read_sql_query(sql_, con_engine)


    del df_data['id']
    del df_data['screen_name']
    del df_data['source']
    del df_data['in_reply_to_screen_name']
    del df_data['in_reply_to_status_id_str']
    del df_data['retweet_count']
    del df_data['favorite_count']
    # Delete useless columns

    df_sort = df_data.sort_values('userid_str')
    # Sort by userid, which is equivalent to automatic classification
    user_list = df_sort['userid_str'].to_list() # 变成列表方便
    time_list = df_sort['created_at'].to_list()
    text_list = df_sort['text'].to_list()

    # Turn all the time into a date
    time_list = [i.date() for i in time_list]

    # Initialize some lists
    user_result = []
    time_result = []
    text_result = []

    aready = []
    #Classify according to each id, fill in the time and text

    for i in range(len(user_list)) :
        if i not in aready:
            time_now = time_list[i]
            aready.append(i)
            user_result.append(user_list[i])
            tem_time_list = [time_list[i]]
            tem_text_list = [text_list[i]]
            for j in range(len(user_list)):
                if j not in aready:
                    time_tem = time_list[j]
                    if user_list[j] == user_list[i] and time_now == time_tem:
                        tem_time_list.append(time_list[j])
                        tem_text_list.append(text_list[j])
                        aready.append(j)
            time_result.append(tem_time_list)
            text_result.append(tem_text_list)

    text_clean_list = copy.deepcopy(text_result)

    for i in range(len(text_clean_list)):
        for j in range(len(text_clean_list[i])):
            text_clean_list[i][j] = pre_processing(text_clean_list[i][j])
            print(text_clean_list[i][j])

    df_tem_1 = pd.DataFrame({'user_id':user_result,
                            'time':time_result,
                            'text':text_result,
                            'perticiple':text_clean_list})




    # Set sparse=False to get the result in the form of numpy ndarray
    v = DictVectorizer(sparse=False)
    word_pre = []
    all_word = []
    # Process text
    # Put the text of the same user together
    for i in range(len(text_clean_list)):
        for j in range(len(text_clean_list[i])):
            for z in text_clean_list[i][j]:
                all_word.append(z)
    # print(all_word)
    # Remove duplicate words
    all_word = set(all_word)
    tem_dict = {}
    # Store words in the form of a dictionary, word as key, frequency as value
    for i in all_word:
        tem_dict[i] = 0

    #
    for i in range(len(text_clean_list)):
        # Make a deep copy to prevent changing the original data
        tem_dict_i = copy.deepcopy(tem_dict)
        for j in range(len(text_clean_list[i])):
            for z in text_clean_list[i][j]:
                tem_dict_i[z] = text_clean_list[i][j].count(z)
        word_pre.append(tem_dict_i)
    # print(word_pre)
    # print(len(word_pre))


    df_tem_1['word_pre'] = word_pre


    # De-duplication
    user_id_set = set(df_tem_1['user_id'].to_list())
    text_list_2 = []
    word_freq = []
    # list of key
    first_pre = list(df_tem_1['word_pre'][0].keys())

    # Process df_tem_2
    # Put all the time of the same user together

    for user in user_id_set:
        # user_id column in dataframe
        tem_df = df_tem_1[df_tem_1['user_id']==user]
        tem_text = ''
        tem_word_freq = {}
        for key in first_pre:
            # set each value to 0
            tem_word_freq[key] = 0

        # get all word
        for text in tem_df['text']:
            for j in text:
                tem_text += j

        for i in tem_df['word_pre']:
            for j in first_pre:
                tem_word_freq[j] += i[j]
        text_list_2.append(tem_text)
        word_freq.append(tem_word_freq)


    df_tem_2 = pd.DataFrame({'user_id':list(user_id_set),
                            'text':text_list_2,
                            'word_freq':word_freq
                            })
    # df_tem_2.to_csv('4.csv')
    # df_tem_1.to_csv('3.csv')

    time_orin_list = df_tem_1['time'].to_list()
    for j in range(len(time_orin_list)):
        time_orin_list[j] = time_orin_list[j][0]


    df_tem_1['time'] = time_orin_list
    del df_tem_1['text']
    del df_tem_1['perticiple']
    df_tem_1.to_csv('./doc/6_l.csv')

    del df_tem_2['text']
    df_tem_2.to_csv('./doc/7_l.csv')

