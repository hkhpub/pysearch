# coding=utf-8

import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import sqlite3
import re

# 무시할 단어 목록을 생성함
ignorewords = set(['the', 'of', 'to', 'and', 'a', 'in', 'is', 'it'])


class crawler:
    # 데이터베이스 이름으로 크롤러를 초기화함
    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)

    def __del__(self):
        self.conn.close()

    def dbcommit(self):
        self.conn.commit()

    # 항목번호를 얻고 등재되지 않았다면 추가하는 보조 함수
    def getentryid(self, table, field, value, createnew=True):
        return None

    # 개별 페이지를 색인함
    def addtoindex(self, url, soup):
        print 'Indexing %s' % url

    # HTML 페이지에서 텍스트 추출함(태그 추출은 안함)
    def gettextonly(self, soup):
        v = soup.string
        if v is None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext+'\n'
            return resulttext
        else:
            print v.strip()
            return v.strip()

    # 공백문자가 아닌 문자들로 단어들을 분리함
    def separatewords(self, text):
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s!='']

    # 이미 색인한 주소라면 true를 리턴
    def isindexed(self, url):
        return False

    # 두 페이지 간의 링크를 추가
    def addlinkref(self, urlFrom, urlTo, linkText):
        pass

    # 페이지 목록으로 시작해서 넓이 우선 검색을 주어진 깊이만큼 수행함.
    # 그 페이지들을 색인함
    def crawl(self, pages, depth=2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                except:
                    print "Could not open %s" % page
                    continue
                soup = BeautifulSoup(c.read())
                self.addtoindex(page, soup)
                links = soup('a')
                for link in links:
                    if ('href' in dict(link.attrs)):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0] # location 부분을 제거함
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                            print "Adding new page url: " + url
                        linkText = self.gettextonly(link)
                        self.addlinkref(page, url, linkText)

                self.dbcommit()

            pages = newpages

    # 데이터베이스 테이블을 생성함.
    def createindextables(self):
        self.conn.execute('create table urllist(url)')
        self.conn.execute('create table wordlist(word)')
        self.conn.execute('create table wordlocation(urlid, wordid, location)')
        self.conn.execute('create table link(fromid integer, toid integer)')
        self.conn.execute('create table linkwords(wordid, linkid)')
        self.conn.execute('create index wordidx on wordlist(word)')
        self.conn.execute('create index urlidx on urllist(url)')
        self.conn.execute('create index wordurlidx on wordlocation(wordid)')
        self.conn.execute('create index urltoidx on link(toid)')
        self.conn.execute('create index urlfromidx on link(fromid)')
        self.dbcommit()

        pass

if __name__ == "__main__":
    crawler = crawler('searchengine.db')
    pagelist = ['http://www.114114.com']
    crawler.crawl(pagelist)