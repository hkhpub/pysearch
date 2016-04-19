# coding=utf-8

import urllib2
from bs4 import BeautifulSoup
from urlparse import urljoin
import sqlite3
import re
import time

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
        cur = self.conn.execute(
            "select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.conn.execute(
                "insert into %s (%s) values ('%s')" % (table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    # 개별 페이지를 색인함
    def addtoindex(self, url, soup):
        if self.isindexed(url):
            return
        print 'Indexing %s' % url

        # 개별 단어를 추출
        text = self.gettextonly(soup)
        words = self.separatewords(text)

        # URL id 추출
        urlid = self.getentryid('urllist', 'url', url)

        # 각 단어를 이 url에 연결함
        for i in range(len(words)):
            word = words[i]
            if word in ignorewords:
                continue
            print 'adding: %s' % word
            wordid = self.getentryid('wordlist', 'word', word)
            self.conn.execute("insert into wordlocation(urlid, wordid, location) "
                              "values (%d, %d, %d)" % (urlid, wordid, i))

    # HTML 페이지에서 텍스트 추출함(태그 추출은 안함)
    def gettextonly(self, soup):
        v = soup.string
        if v is None:
            c = soup.contents
            resulttext = ''
            for t in c:
                subtext = self.gettextonly(t)
                resulttext += subtext + '\n'
            return resulttext
        else:
            # print v.strip()
            return v.strip()

    # 공백문자가 아닌 문자들로 단어들을 분리함
    def separatewords(self, text):
        print 'text: %s' % text
        splitter = re.compile('\\W*')
        words = [s.lower() for s in splitter.split(text) if s != '']
        return words

    # 이미 색인한 주소라면 true를 리턴
    def isindexed(self, url):
        u = self.conn.execute(
            "select rowid from urllist where url='%s'" % url).fetchone()
        if u is not None:
            # 이미 크롤되었는지 점검함
            v = self.conn.execute(
                "select * from wordlocation where urlid=%d" % u[0]).fetchone()
            if v is not None:
                return True
        return False

    # 두 페이지 간의 링크를 추가
    def addlinkref(self, url_from, url_to, link_text):
        words = self.separatewords(link_text)
        fromid = self.getentryid('urllist', 'url', url_from)
        toid = self.getentryid('urllist', 'url', url_to)
        if fromid == toid:
            return
        cur = self.conn.execute("insert into link(fromid, toid) values (%d, %d)" % (fromid, toid))
        linkid = cur.lastrowid
        for word in words:
            if word in ignorewords:
                continue
            wordid = self.getentryid('wordlist', 'word', word)
            self.conn.execute("insert into linkwords(linkid, wordid) values (%d, %d)" % (linkid, wordid))
        pass

    # 페이지 목록으로 시작해서 넓이 우선 검색을 주어진 깊이만큼 수행함.
    # 그 페이지들을 색인함
    def crawl(self, pages, depth=3):
        for i in range(depth):
            newpages = set()
            for page in pages:
                try:
                    c = urllib2.urlopen(page)
                    time.sleep(10 / 1000)
                except:
                    print "Could not open %s" % page
                    continue
                soup = BeautifulSoup(c.read())
                self.addtoindex(page, soup)
                links = soup('a')
                for link in links:
                    if 'href' in dict(link.attrs):
                        url = urljoin(page, link['href'])
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]  # location 부분을 제거함
                        if url[0:4] == 'http' and not self.isindexed(url):
                            newpages.add(url)
                            print "Adding new page url: " + url
                        linktext = self.gettextonly(link)
                        self.addlinkref(page, url, linktext)
                self.dbcommit()
            pages = newpages

    # 페이지 랭크 계산
    def calculatepagerank(self, iterations=20):
        # 현 페이지랭크 테이블을 지움
        self.conn.execute('drop table if exists pagerank')
        self.conn.execute('create table pagerank(urlid primary key, score)')

        # 모든 url의 페이지랭크 값을 1로 초기화함
        self.conn.execute('insert into pagerank select rowid, 1.0 from urllist')
        self.dbcommit()

        for i in range(iterations):
            print "Iteration %d" % (i)
            for (urlid,) in self.conn.execute('select rowid from urllist'):
                pr = 0.15

                # 이 페이지에 링크를 가진 모든 페이지들에 대해 looping
                for (linker, ) in self.conn.execute(
                                "select distinct fromid from link where toid=%d" % urlid):
                    # 링크 페이지의 페이지랭크를 얻음
                    linkingpr = self.conn.execute(
                        "select score from pagerank where urlid=%d" % linker).fetchone()[0]

                    # 링크 페이지의 전체 랭크 수를 얻음
                    linkingcount = self.conn.execute(
                        "select count(*) from link where fromid=%d" % linker).fetchone()[0]

                    pr += 0.85 * (linkingpr/linkingcount)

                self.conn.execute(
                    "update pagerank set score=%f where urlid=%d" % (pr, urlid))
            self.dbcommit()

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


class searcher:
    def __init__(self, dbname):
        self.conn = sqlite3.connect(dbname)

    def __del__(self):
        self.conn.close()

    def getmatchrows(self, q):
        # 검색어 생성용 문자열
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []
        rows = []       # 결과

        # 공백으로 단어들을 분리함
        words = q.split(' ')
        tablenumber = 0

        for word in words:
            # 단어 ID 구함
            wordrow = self.conn.execute(
                "select rowid from wordlist where word='%s' " % word).fetchone()
            if wordrow is not None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += 'w%d.urlid=w%d.urlid and ' % (tablenumber-1, tablenumber)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += 'w%d.wordid=%d' % (tablenumber, wordid)
                tablenumber += 1

        if len(wordids) > 0:
            # 분리된 단편에서 쿼리를 만듦
            fullquery = "select %s from %s where %s" % (fieldlist, tablelist, clauselist)
            cur = self.conn.execute(fullquery)
            rows = [row for row in cur]
        else:
            print 'no matched words'

        return rows, wordids

    def getscoredlist(self, rows, wordids):
        totalscores = dict([(row[0], 0) for row in rows])

        # 이후 득점 함수를 넣을 위치임
        # weights = [(1.0, self.frequencyscore(rows))]
        # weights = [(1.0, self.locationscore(rows))]
        # weights = [(1.0, self.distancescore(rows))]
        weights = [(1.0, self.locationscore(rows)),
                   (1.0, self.frequencyscore(rows)),
                   (1.0, self.pagerankscore(rows)),
                   (1.0, self.linktextscore(rows, wordids))]

        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]

        return totalscores

    def geturlname(self, id):
        return self.conn.execute(
            "select url from urllist where rowid=%d" % id).fetchone()[0]

    def query(self, q):
        rows, wordids = self.getmatchrows(q)
        scores = self.getscoredlist(rows, wordids)
        rankedscores = sorted([(score, url) for (url, score) in scores.items()], reverse=1)
        for (score, urlid) in rankedscores[0:10]:
            print '%f\t%s' % (score, self.geturlname(urlid))

    def normalizescores(self, scores, small_is_better=0):
        # 0으로 나누는 오류를 피함
        vsmall = 0.0001
        if small_is_better:
            minscore = min(scores.values())
            return dict([(u, float(minscore)/max(vsmall, l)) for (u, l) in scores.items()])
        else:
            maxscore = max(scores.values())
            if maxscore == 0:
                maxscore = vsmall
            return dict([(u, float(c)/maxscore) for (u, c) in scores.items()])

    # 단어 빈도에 따른 scoring 함수
    def frequencyscore(self, rows):
        counts = dict([(row[0], 0) for row in rows])
        for row in rows:
            counts[row[0]] += 1
        return self.normalizescores(counts)

    # 문서 내 위치 scoring 함수
    def locationscore(self, rows):
        locations = dict([(row[0], 1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < locations[row[0]]:
                locations[row[0]] = loc

        return self.normalizescores(locations, small_is_better=1)

    # 단어 거리 scoring 함수
    def distancescore(self, rows):
        # 한 단어만 있으면 모두 선택함
        if len(rows[0]) <= 2:
            return dict([(row[0], 1.0) for row in rows])

        # 큰 값들로 딕셔너리를 초기화함
        mindistance = dict([(row[0], 1000000) for row in rows])

        for row in rows:
            dist = sum([abs(row[i] - row[i-1]) for i in range(2, len(row))])
            if dist < mindistance[row[0]]:
                mindistance[row[0]] = dist
        return self.normalizescores(mindistance, small_is_better=1)

    def inboundlinkscore(self, rows):
        uniqueurls = set([row[0] for row in rows])
        inboundcount = dict([(u, self.conn.execute(
            "select count(*) from link where toid=%d" % u).fetchone()[0])
                for u in uniqueurls])
        return self.normalizescores(inboundcount)

    # 페이지 랭크 scoring 함수
    def pagerankscore(self, rows):
        pageranks = dict([(row[0],
                           self.conn.execute("select score from pagerank where urlid=%d" % row[0]).fetchone()[0])
                          for row in rows])
        maxrank = max(pageranks.values())
        normalizedscores = dict([(u, float(l)/maxrank) for (u, l) in pageranks.items()])
        return normalizedscores

    def linktextscore(self, rows, wordids):
        linkscores = dict([(row[0], 0) for row in rows])
        for wordid in wordids:
            cur = self.conn.execute("select link.fromid, link.toid from linkwords, link"
                                    " where wordid=%d and linkwords.linkid=link.rowid" % wordid)
            for (fromid, toid) in cur:
                if toid in linkscores:
                    pr = self.conn.execute("select score from pagerank where urlid=%d" % fromid).fetchone()[0]
                    linkscores[toid] += pr
            maxscore = max(linkscores.values())
            if maxscore == 0:
                maxscore = 0.000001
            normalizedscores = dict([(u, float(l)/maxscore) for (u, l) in linkscores.items()])
            return normalizedscores

if __name__ == "__main__":
    crawler = crawler('searchindex.db')
    # crawler.createindextables()
    # pagelist = ['http://www.wikipedia.org']
    # crawler.crawl(pagelist)

    # crawler.calculatepagerank()

    searcher = searcher('searchindex.db')
    print searcher.query('english art')

