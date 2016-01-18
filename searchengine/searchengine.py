import sqlite3 as sqlite
class searcher:
    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)

    def __def__(self) :
        self.con.close()

    # search the rows matching corresponding query string
    def getmatchrows(self,querystring):
        #build the query
        fieldlist = 'w0.urlid'
        tablelist = ''
        clauselist = ''
        wordids = []

        #split the words by whitespace
        words = querystring.split(' ')
        tablenumber = 0

        for word in words:
            #getwordid
            wordrow = self.con.execute("select rowid from wordlist where word='%s'" % word).fetchone()
            print "wordrow:",wordrow
            if wordrow != None:
                wordid = wordrow[0]
                wordids.append(wordid)
                if tablenumber > 0:
                    tablelist += ','
                    clauselist += ' and '
                    clauselist += "w%d.urlid=w%d.urlid and " % (tablenumber, tablenumber-1)
                fieldlist += ',w%d.location' % tablenumber
                tablelist += 'wordlocation w%d' % tablenumber
                clauselist += "w%d.wordid=%d" % (tablenumber,wordid)
                tablenumber+=1
        # create query from separate parts
        fullquery = 'select %s from %s where %s' % (fieldlist, tablelist, clauselist)
        print "fullquery:",fullquery
        cur = self.con.execute(fullquery)
        rows = [row for row in cur]
        return rows, wordids

    def frequency(self,rows) :
        count = dict([(row[0], 0) for row in rows])
        for row in rows:
            count[row[0]] += 1
        return self.getnormalizedscores(count)

    def getnormalizedscores(self,scores,SMALL=False) :
        vsmall = 0.00001 # avoid dividing zero
        if SMALL:
            minscore = min(scores.values())
            return dict([(u,float(minscore)/max(vsmall,l)) for (u,l) in scores.items()])
        else :
            maxscore = max(scores.values())
            if maxscore==0:
                maxscore = vsmall
            return dict([(u,float(c)/maxscore) for (u,c) in scores.items()])

    def getscoredlist(self,rows,wordids) :
        totalscores = dict([(row[0],0) for row in rows])

        #parameters in weighting function
        weights = [(1.0,self.frequency(rows))]

        for (weight,scores) in weights:
            for url in totalscores:
                totalscores[url] += weight*scores[url]
        return totalscores

    def geturlname(self, urlid) :
        return self.con.execute("select url from urllist where rowid=%d" % urlid).fetchone()[0]

    def query(self, querystring) :
        r, w = self.getmatchrows(querystring)
        ts = self.getscoredlist(r,w)
        ranked = sorted([(score,url) for (url, score) in ts.items()],reverse=1)
        for s, i in ranked:
            print "url:%s, score:%f" % (self.geturlname(i), s)

