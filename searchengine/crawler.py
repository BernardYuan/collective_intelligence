import urllib2
from BeautifulSoup import *
from urlparse import urljoin
import sqlite3 as sqlite
import re

ignoredwords = set(["a","an","the","of","to","from","and","in","it","is"])
class crawler:
    def __init__(self,dbname) :
        self.con = sqlite.connect(dbname)

    def __del__(self) :
        self.con.close()

    def dbcommit(self) :
        self.con.commit()

    #get an entry id and add it
    def getentryid(self, table, field, value, createNew=True) :
        cur = self.con.execute("select rowid from %s where %s='%s'" % (table, field, value))
        res = cur.fetchone()
        if res == None:
            cur = self.con.execute("insert into %s(%s) values('%s')" % (table, field, value))
            return cur.lastrowid
        else :
            return res[0]

    #indexing individual page
    def addtoindex(self, url, soup) :
        if self.isindexed(url) :
            return
        # get separated words
        text = self.gettextonly(soup)
        words = self.separatewords(text)

        #get URL id
        urlid = self.getentryid('urllist','url',url)

        #link each word to this url
        for i in range(len(words)):
            word = words[i]
            if word in ignoredwords:
                continue
            wordid = self.getentryid('wordlist','word',word)
            self.con.execute("insert into wordlocation(urlid, wordid, location) values (%d,%d,%d)" % (urlid, wordid, i))



    # extract text from the page, no HTML tags
    def gettextonly(self,soup) :
        v = soup.string
        if v is None:
            ctt = soup.contents
            # members in list ctt is also 'soup type'
            result = ''
            for i in ctt:
                subresult = self.gettextonly(i)
                result += subresult+'\n'
            return result
        else :
            return v.strip()


    # separate words by non-whitespace character
    def separatewords(self,text) :
        splitter = re.compile('\\W*')
        return [s.lower() for s in splitter.split(text) if s != '']

    # judge whether an url is indexed
    def isindexed(self, url) :
        cur = self.con.execute("select rowid from urllist where url='%s'" % url).fetchone()
        if cur is not None:
            #check if is really crawled
            vrf = self.con.execute("select * from wordlocation where urlid='%d'" % cur[0]).fetchone()
            if vrf is not None:
                return True
        return False

    # add links between two url
    def addlinkref(self, urlFrom, urlTo, text):
        pass

    # start with a list of pages, do breadth first search to the designated depth
    # pages = [list of pages]
    # depth: int, depth of BFS
    def crawl(self,pages, depth=2):
        for i in range(depth):
            newpages = set()
            for page in pages:
                if self.isindexed(page):
                    continue
                try:
                    p = urllib2.urlopen(page)
                except:
                    print "Unable to open %s" % page
                    continue
                soup = BeautifulSoup(p.read())
                self.addtoindex(page,soup)

                links = soup('a')
                for link in links:
                    if 'href' in dict(link.attrs):
                        url = urljoin(page,link["href"])
                        url = url.split("#")[0]  #remove location portion
                        if url[0:4]=='http' and not self.isindexed(url):
                            newpages.add(url)
                        linkText = self.gettextonly(link)
                        self.addlinkref(page,url,linkText)
                self.dbcommit()
            pages = newpages

    # create database tables
    def createindextables(self) :
        self.con.execute("create table urllist(url)")
        self.con.execute("create table wordlist(word)")
        self.con.execute("create table wordlocation(urlid, wordid, location)")
        self.con.execute("create table link(fromid integer, toid integer)")
        self.con.execute("create table linkword(wordid, linkid)")
        self.con.execute("create index wordidx on wordlist(word)")
        self.con.execute("create index urlidx on urllist(url)")
        self.con.execute("create index wordurlidx on wordlocation(wordid)")
        self.con.execute("create index urltoidx on link(toid)")
        self.con.execute("create index urlfromidx on link(fromid)")
        self.dbcommit()
