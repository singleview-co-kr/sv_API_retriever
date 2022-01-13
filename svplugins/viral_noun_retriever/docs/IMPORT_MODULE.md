how to word cloud
https://konlpy.org/ko/v0.5.2/install/
https://liveyourit.tistory.com/58

Install KoNLPy on CentOS 7, 8
# yum install -y gcc-c++
# yum install -y java-1.8.0-openjdk-devel
# python3.x -m pip install --upgrade pip
# python3.x -m pip install konlpy
# yum install curl git
# bash <(curl -s https://raw.githubusercontent.com/konlpy/konlpy/master/scripts/mecab.sh)

install wordcloud 
# pip3.x install wordcloud

how to install customized_konlpy
https://inspiringpeople.github.io/data%20analysis/ckonlpy/
https://pypi.org/project/customized-KoNLPy/

# git clone https://github.com/lovit/customized_konlpy.git
# pip3.x install customized_konlpy

error case #1:
AttributeError: module 'tweepy' has no attribute 'StreamListener'

# vi /usr/local/lib/python3.7/site-packages/konlpy/stream/twitter.py 
edit line 17

from
class CorpusListener(tweepy.StreamListener):

to
class CorpusListener(tweepy.Stream):