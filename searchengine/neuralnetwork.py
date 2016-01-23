from math import tanh
import sqlite3 as sqlite
def dtanh(y) :
    return 1.0 - y*y

class searchnet:
    def __init__(self, dbname) :
        self.con = sqlite.connect(dbname)

    def __del__(self, dbname) :
        self.con.close()

    def createtables(self) :
        # create tables
        self.con.execute('create table hiddennode(create_key)')
        self.con.execute('create table wordhidden(fromid, toid, strength)')
        self.con.execute('create table urlhidden(fromid, toid, strength)')

        #create indices
        self.con.execute('create index hiddennodeindex on hiddennode(create_key)')
        self.con.execute('create index hiddenfromwordindex on wordhidden(fromid)')
        self.con.execute('create index hiddentowordindex on wordhidden(toid)')
        self.con.execute('create index hiddenfromurlindex on urlhidden(fromid)')
        self.con.execute('create index hiddentourlindex on urlhidden(toid)')

        self.con.commit()

    def getstrength(self, fromid, toid, layer) :
        if layer==0 :
            table = 'wordhidden'
        else :
            table = 'urlhidden'

        res = self.con.execute('select strength from %s where fromid=%d and toid=%d' % (table, fromid, toid)).fetchone()
        if res is None :
            if layer==0 :
                return -0.2
            else :
                return 0
        return res[0]

    def setstrength(self, fromid, toid, layer, strength) :
        if layer==0 :
            table = 'wordhidden'
        else :
            table = 'urlhidden'

        res = self.con.execute('select rowid from %s where fromid=%d and toid=%d' % (table, fromid, toid)).fetchone()

        if res is None :
            self.con.execute('insert into %s(fromid,toid,strength) values(%d, %d, %f)' % (table, fromid, toid, strength))
        else :
            row = res[0]
            self.con.execute('update %s set strength=%f where rowid=%d' % (table, strength, row))

    def generatehiddennode(self, wordids, urls) :
        if len(wordids) > 3 :
            return None

        # check if the node for this combination of words already exist
        createkey = '_'.join(sorted([str(w) for w in wordids]))
        res = self.con.execute("select rowid from hiddennode where create_key='%s'" % createkey).fetchone()

        # if not, create it
        if res is None :
            cur = self.con.execute("insert into hiddennode(create_key) values('%s')" % createkey)
            hiddenid = cur.lastrowid
            #set a default weight
            for wordid in wordids :
                self.setstrength(wordid,hiddenid,0,1.0/len(wordids))
            for url in urls :
                self.setstrength(hiddenid, url, 1, 0.1)
            self.con.commit()

    def getallhiddenids(self, wordids, urlids) :
        l1 = set()
        for wordid in wordids:
            cur = self.con.execute("select toid from wordhidden where fromid=%d" % wordid)
            for row in cur:
                l1.add(row[0])

        for urlid in urlids:
            cur = self.con.execute("select fromid from urlhidden where toid=%d" % urlid)
            for row in cur:
                l1.add(row[0])

        return [i for i in l1]

    def setupnetwork(self, wordids, urlids) :
        #value list
        self.wordids = wordids
        self.urlids = urlids
        self.hiddenids = self.getallhiddenids(wordids, urlids)

        #node outputs(arguments)
        self.ai = [1.0] * len(self.wordids)
        self.ah = [1.0] * len(self.hiddenids)
        self.ao = [1.0] * len(self.urlids)

        # weight matrix
        self.wi = [[self.getstrength(wordid, hiddenid, 0) for hiddenid in self.hiddenids] for wordid in self.wordids]
        self.wo = [[self.getstrength(hiddenid, urlid, 1) for urlid in self.urlids] for hiddenid in self.hiddenids]

    def feedforward(self) :
        # inputs are query words
        for i in range(len(self.wordids)) :
            self.ai[i] = 1.0

        # activate the hidden nodes
        for j in range(len(self.hiddenids)) :
            sm = 0.0
            for i in range(len(self.wordids)) :
                sm += self.ai[i] * self.wi[i][j]
            self.ah[j] = tanh(sm)

        # activate outputs
        for k in range(len(self.urlids)) :
            sm = 0.0
            for j in range(len(self.hiddenids)) :
                sm += self.ah[j] * self.wo[j][k]
            self.ao[k] = tanh(sm)

        return self.ao[:]

    def getresult(self, wordids, urlids) :
        self.setupnetwork(wordids, urlids)
        return self.feedforward()

    def backpropagation(self, targets, N = 0.5) :
        # output layer
        output_deltas = [0.0]*len(self.urlids)
        for k in range(len(self.urlids)) :
            error = targets[k] - self.ao[k]
            output_deltas[k] = dtanh(self.ao[k]) * error

        # hidden layer
        hidden_deltas = [0.0]*len(self.hiddenids)
        for j in range(len(self.hiddenids)) :
            error = 0.0
            for k in range(len(self.urlids)) :
                error += output_deltas[k] * self.wo[j][k]
            hidden_deltas[j] = dtanh(self.ah[j]) * error

        #update output weights
        for j in range(len(self.hiddenids)) :
            for k in range(len(self.urlids)) :
                change = output_deltas[k] * self.ah[j]
                self.wo[j][k] += N*change

        #update input weights
        for i in range(len(self.wordids)) :
            for j in range(len(self.hiddenids)) :
                change = hidden_deltas[j] * self.ai[i]
                self.wi[i][j] += N*change
    def updatedatabase(self) :
        # setup database values
        for i in range(len(self.wordids)) :
            for j in range(len(self.hiddenids)) :
                self.setstrength(self.wordids[i],self.hiddenids[j],0,self.wi[i][j])

        for j in range(len(self.hiddenids)) :
            for k in range(len(self.urlids)) :
                self.setstrength(self.hiddenids[j],self.urlids[k],1,self.wo[j][k])
        self.con.commit()

    def trainquery(self,wordids, urlids, selectedurl) :
        # generate hidden node if necessary
        self.generatehiddennode(wordids, urlids)

        self.setupnetwork(wordids, urlids)
        self.feedforward()
        targets = [0.0] * len(urlids)
        targets[urlids.index(selectedurl)] = 1.0
        self.backpropagation(targets)
        self.updatedatabase()


