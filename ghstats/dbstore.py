import motor.motor_asyncio


class DbStore(object):
    HOSTNAME = '127.0.0.1'
    HOSTPORT = 27017
    DB_NAME = 'ghstats'
    def __init__(self, **kwargs):
        self.hostname = kwargs.get('hostname', self.HOSTNAME)
        self.hostport = kwargs.get('hostport', self.HOSTPORT)
        self.db_name = kwargs.get('db_name', self.DB_NAME)
        self._client = None
        self._db = None
    @property
    def client(self):
        c = self._client
        if c is None:
            c = self._client = motor.motor_asyncio.AsyncIOMotorClient(
                self.hostname, self.hostport,
            )
        return c
    @property
    def db(self):
        db = self._db
        if db is None:
            c = self.client
            db = self._db = c[self.db_name]
        return db
    def get_collection(self, name):
        return self.db[name]
    async def get_doc(self, collection_name, filt, *args, **kwargs):
        coll = self.get_collection(collection_name)
        return await coll.find_one(filt, *args, **kwargs)
    async def add_doc(self, collection_name, doc):
        coll = self.get_collection(collection_name)
        return await coll.insert_one(doc)
    async def add_doc_if_missing(self, collection_name, filt, doc):
        coll = self.get_collection(collection_name)
        existing = await coll.count(filt)
        if existing:
            return existing
        return await coll.insert_one(doc)
    async def update_doc(self, collection_name, filt, doc):
        coll = self.get_collection(collection_name)
        old_doc = await self.get_doc(collection_name, filt)
        if old_doc is None:
            return await self.add_doc(collection_name, doc)
        _id = old_doc['_id']
        return await coll.replace_one({'_id':_id}, doc)
