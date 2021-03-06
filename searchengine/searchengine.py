import sqlite3 as sqlite
import neuralnetwork as nn
class searcher:
    def __init__(self,dbname):
        self.con = sqlite.connect(dbname)
        self.net = nn.searchnet(dbname)

    def __del__(self) :
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
    def locationscore(self,rows) :
        location = dict([(row[0],1000000) for row in rows])
        for row in rows:
            loc = sum(row[1:])
            if loc < location[row[0]]:
                location[row[0]] = loc
        return self.getnormalizedscores(location, SMALL=True)
    def distancescore(self,rows) :
        if len(rows[0]) <=2:
            return dict([(row[0],1.0) for row in rows])
        mindist = dict([(row[0],1000000) for row in rows])
        for row in rows:
            dist = sum([abs(row[i]-row[i-1]) for i in range(2, len(row))])
            if dist < mindist[row[0]]:
                mindist[row[0]] = dist
        return self.getnormalizedscores(mindist, SMALL=True)
    def pagerankscore(self, rows) :
        prs = dict([(row[0],self.con.execute("select score from pagerank where urlid = %d" % row[0]).fetchone()[0]) for row in rows])
        print prs
        maxrank = max(prs.values())
        norm = dict([(u,1.0*l/maxrank) for (u,l) in prs.items()])
        return norm
    def nnscore(self, rows, wordids) :
        # get unique url ids
        urlids = [urlid for urlid in set([row[0] for row in rows])]
        nnres = self.net.getresult(wordids, urlids)
        scores = dict([(urlids[i], nnres[i]) for i in range(len(urlids))])
        return self.getnormalizedscores(scores)


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
        weights = [(1.0,self.frequency(rows)),
                (1.0,self.locationscore(rows)),
                (1.0,self.distancescore(rows)),
                (1.0,self.pagerankscore(rows)),
                (1.0,self.nnscore(rows,wordids))]

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
        # for s, i in ranked:
            # print "url:%s, score:%f" % (self.geturlname(i), s)
        # return the wordid list and the top-10 ranked pages
        return w, [r[1] for r in ranked[0:10]]

