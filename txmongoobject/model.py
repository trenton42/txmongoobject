import txmongo
import pytz
import json
import iso8601
from txmongo import connection
from collections import OrderedDict
try:
    from txmongo._pymongo.objectid import ObjectId, InvalidId
except ImportError:
    from bson.objectid import ObjectId, InvalidId
from twisted.internet import defer
from datetime import datetime


def _all_subclasses(cls):
    return cls.__subclasses__() + [g for s in cls.__subclasses__() for g in _all_subclasses(s)]


class MongoEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return {"$oid": str(obj)}
        if isinstance(obj, list):
            return [self.default(i) for i in obj]
        if isinstance(obj, datetime):
            return {"$date": obj.isoformat()}
        return obj


class notLoadedError(Exception):
    pass


class DocumentExists(Exception):
    pass


class metaMongoObj(type):
    def __new__(meta, classname, bases, classDict):
        classDict['_id'] = mongoidProperty()
        classDict['cdate'] = dateProperty()
        if "collection" not in classDict:
            collection = classname
            for i in bases:
                if i.__name__ == "MongoObj" or not hasattr(i, "collection"):
                    continue
                collection = i.collection
            classDict['collection'] = collection
        if classDict["collection"] != classname:
            classDict["_unmarshal_class"] = stringProperty(default=classname)

        for k, v in classDict.iteritems():
            if not issubclass(v.__class__, mongoProperty):
                continue
            v._name = v._key if v._key else k
        return type.__new__(meta, classname, bases, classDict)


class mongoProperty(object):

    value = None
    _name = None
    _key = None

    def __init__(self, allowNone=True, default=None, key=None):
        self.allowNone = allowNone
        self.default = default
        if key:
            self._key = key

    def set(self, value):
        if value is None and self.default is not None:
            value = self.default
        return value

    def get(self, value):
        return value

    def __set__(self, instance, value):
        if not self._name:
            return
        if instance.loaded and (self._name not in instance._prop_data or instance._prop_data[self._name] != self.set(value)):
            instance._prop_dirty.add(self._name)
        instance._prop_data[self._name] = self.set(value)

    def __get__(self, instance, owner):
        if self._name not in instance._prop_data:
            val = self.get(self.default)
            instance._prop_data[self._name] = val
            return val
        return self.get(instance._prop_data[self._name])


class boolProperty(mongoProperty):

    def set(self, value):
        return bool(value)


class stringProperty(mongoProperty):

    def __init__(self, maxLength=None, **kwargs):
        if maxLength is not None:
            if maxLength.__class__ is not int or maxLength <= 0:
                maxLength = None

        self.maxLength = maxLength
        super(stringProperty, self).__init__(**kwargs)

    def set(self, value):
        if value is None and self.default is not None:
            value = self.default

        if value.__class__ is unicode:
            value = value.encode('utf-8')
        elif value is None:
            value = None if self.allowNone else ''
        elif value.__class__ is not str:
            value = str(value)
        return value

    def get(self, value):
        if value is not None and not isinstance(value, unicode):
            value = unicode(value, 'utf-8')
        return value


class dateProperty(mongoProperty):

    def set(self, value):
        if not isinstance(value, datetime):
            value = None
        elif value.tzinfo and value.tzinfo != pytz.utc:
            value = value.astimezone(pytz.utc)

        return value

    def __get__(self, instance, owner):
        if self._name not in instance._prop_data:
            return self.default

        value = self.get(instance._prop_data[self._name])

        if not isinstance(value, datetime):
            return value

        if instance.display_timezone:
            value = value.replace(tzinfo=pytz.utc)
            return value.astimezone(instance.display_timezone)

        return value


class intProperty(mongoProperty):

    def __init__(self, unsigned=False, **kwargs):
        self.unsigned = unsigned
        super(intProperty, self).__init__(**kwargs)

    def set(self, value):
        if value.__class__ is unicode:
            value = value.encode('utf-8')

        if value.__class__ is int:
            pass
        elif value.__class__ is float:
            value = int(round(value))
        elif value.__class__ is str:
            try:
                value = int(value)
            except ValueError:
                # Some invalid stuff passed in the string.
                value = None
        else:
            # Handle everything else that was passed.
            value = None

        if value is None and self.default is not None:
            value = self.default

        if not self.allowNone and value is None:
            value = 0

        if self.unsigned and value is not None and value < 0:
            value = abs(value)

        return value


class floatProperty(mongoProperty):

    def __init__(self, unsigned=False, **kwargs):
        self.unsigned = unsigned
        super(floatProperty, self).__init__(**kwargs)

    def set(self, value):
        if value.__class__ is unicode:
            value = value.encode('utf-8')

        if value.__class__ is int:
            value = float(value)
        elif value.__class__ is float:
            pass
        elif value.__class__ is str:
            try:
                value = float(value)
            except ValueError:
                # Some invalid stuff passed in the string.
                value = None
        else:
            # Handle everything else that was passed.
            value = None

        if value is None and self.default is not None:
            value = self.default

        if not self.allowNone and value is None:
            value = 0.0

        if self.unsigned and value is not None and value < 0:
            value = abs(value)

        return value


class mongoidProperty(mongoProperty):

    def set(self, value):
        if value is None:
            return None
        try:
            value = ObjectId(value)
        except (TypeError, InvalidId):
            value = None
        return value

    def get(self, value):
        return None if value is None else ObjectId(value)


class referenceProperty(mongoProperty):
    """Creates a reference to another mongo object by storing the _id"""

    def __init__(self, cls, multi=False, **kwargs):
        super(referenceProperty, self).__init__(**kwargs)

        if not issubclass(cls, MongoObj):
            raise ValueError('cls must be subclass of MongoObj')
        self._refCls = cls

    def set(self, value):

        if value is None or isinstance(value, ObjectId):
            return value

        if isinstance(value, self._refCls):
            return value

        try:
            value = ObjectId(value)
        except InvalidId:
            err = '%s must be subclass of %s' % (str(value),
                                                 str(self._refCls.__name__))
            raise ValueError(err)

        return value


class dictProperty(mongoProperty):

    def set(self, value):
        if value.__class__ is not dict:
            value = None

        if not self.allowNone and value is None:
            value = {}
        return value

    def get(self, value):
        if not self.allowNone and value is None:
            return {}
        return value


class listProperty(mongoProperty):

    _defaultWrapper = None

    def __init__(self, *args, **kwargs):
        wrapper = kwargs.get('wrapper', None)
        if isinstance(wrapper, type):
            wrapper = wrapper()
        if wrapper is not None and isinstance(wrapper, mongoProperty):
            self._defaultWrapper = wrapper
        if 'wrapper' in kwargs:
            del kwargs['wrapper']
        super(listProperty, self).__init__(*args, **kwargs)

    def set(self, value):
        if value.__class__ is not list:
            value = None
        elif self._defaultWrapper is not None:
            value = map(lambda key: self._defaultWrapper.set(key), value)

        if not self.allowNone and value is None:
            value = {}
        return value

    def _getIds(self, values):
        ''' Get all ObjectIds of members (for serialization) if the
        base wrapper is referenceProperty
        '''
        if not isinstance(self._defaultWrapper, referenceProperty) or values is None:
            return values
        out = []
        for i in values:
            if i is None:
                continue
            if isinstance(i, ObjectId):
                out.append(i)
            else:
                out.append(i._id)
        return out


class geoPointProperty(mongoProperty):
    ''' Point GeoJSON object, with GeoJSON metadata hidden '''
    def set(self, value):
        if isinstance(value, dict) and 'coordinates' in value and 'type' in value:
            # Allow raw GeoJSON to get through
            return value

        if not isinstance(value, list):
            value = [0, 0]
        if len(value) != 2:
            value = [0, 0]

        return {'type': "Point", 'coordinates': value}

    # def get(self, value):
    #     if not value or not isinstance(value, dict) or 'coordinates' not in value:
    #         return [0, 0]

    #     return value['coordinates']


class MongoSubObj(object):

    _prop_data = {}
    _prop_dirty = set()

    def getValues(self):
        ''' Serialize all of the values into a mongoable dict '''
        out = {}

        for k, v in self.schema.iteritems():
            key = v._key if v._key else k
            if isinstance(v, referenceProperty):
                tmp = getattr(self, k)
                if tmp is None or isinstance(tmp, ObjectId):
                    out[key] = tmp
                else:
                    out[key] = tmp._id
            elif isinstance(v, listProperty):
                tmp = getattr(self, k)
                out[key] = v._getIds(tmp)
            elif issubclass(v.__class__, objectProperty):
                out[key] = getattr(self, k).getValues()
            else:
                out[key] = getattr(self, k)

        return out

    def setValues(self, data):
        ''' Set the values of the object recursively '''
        schema = self.schema
        keymap = {}
        for k, v in schema.items():
            key = v._key if v._key else k
            keymap[key] = k
        for k, v in data.iteritems():
            if k not in keymap:
                continue
            setattr(self, keymap[k], v)

    def __iter__(self):
        def iterKeys():
            for i in self.getKeys():
                yield i, getattr(self, i)
        return iterKeys()

    def __len__(self):
        return len(self.getKeys())

    def getKeys(self):
        out = []
        for k, v in self.__class__.__dict__.iteritems():
            if not issubclass(v.__class__, mongoProperty):
                continue
            out.append(k)
        return out

    @defer.inlineCallbacks
    def loadRefs(self):
        ''' Load references that are defined in this object '''

        for k, v in self.schema.iteritems():
            if isinstance(v, listProperty) and isinstance(v._defaultWrapper, referenceProperty):
                val = getattr(self, k)
                if val is None:
                    val = []
                tmp = []
                for i in val:
                    if i is None or isinstance(i, v._defaultWrapper._refCls):
                        tmp.append(i)
                        continue
                    row = yield v._defaultWrapper._refCls().load(i)
                    tmp.append(row)
                setattr(self, k, tmp)
                continue
            if not isinstance(v, referenceProperty):
                continue
            val = getattr(self, k)
            if val is None or isinstance(val, v._refCls):
                continue
            try:
                tmp = yield v._refCls().load(val)
            except KeyError:
                tmp = None
            setattr(self, k, tmp)

    @property
    def schema(self):
        out = {}
        for i in self.__class__.__mro__:
            if not issubclass(i, MongoSubObj):
                continue
            for k, v in i.__dict__.iteritems():
                if not issubclass(v.__class__, mongoProperty):
                    continue
                out[k] = v
        return out

    def create(self, data):
        ''' Called when the object is first created. All data will be
        passed through this function, and should be returned '''
        return data

    def as_json(self):
        return json.dumps(self.getValues(), cls=MongoEncoder)

    @classmethod
    def from_json(cls, data):
        obj = cls()
        out = json.loads(data, object_hook=cls._object_hook)
        obj.setValues(out)
        return obj

    @staticmethod
    def _object_hook(obj):
        if not isinstance(obj, dict):
            return obj
        for k, v in obj.items():
            if isinstance(v, dict):
                if "$oid" in v:
                    obj[k] = ObjectId(v["$oid"])
                elif "$date" in v:
                    obj[k] = iso8601.parse_date(v["$date"])
                else:
                    obj[k] = MongoSubObj._object_hook(v)
            elif isinstance(v, list):
                obj[k] = [MongoSubObj._object_hook(i) for i in v]
        return obj


class objectProperty(mongoProperty):
    ''' An embedded object property '''

    def __init__(self, refClass=None, allowNone=False):
        if not issubclass(refClass, MongoSubObj):
            raise ValueError('refClass must be a subclass of MongoSubObj')
        self.value = refClass()

    def __set__(self, instance, value):
        if self.value:
            self.value.setValues(value)


class MongoObj(MongoSubObj):
    ''' Results class for running a query. Each result is a as
    appropriate mongo object '''

    loaded = False
    dbname = 'brndydb'
    __metaclass__ = metaMongoObj
    mongo = None
    display_timezone = None

    def __init__(self):
        self._id = None
        self._prop_data = {}
        self._prop_dirty = set()
        super(MongoObj, self).__init__()

    @classmethod
    def connect(cls, host, port, username=None, password=None, database=None):
        if cls.mongo is not None:
            # Possibly already connected?
            return

        if database:
            cls.dbname = database

        def _after_connect(res):
            cls.mongo = res
        details = {
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database": cls.dbname
        }
        if username and password:
            uri = "mongodb://{username}:{password}@{host}:{port}/{database}"
        else:
            uri = "mongodb://{host}:{port}/{database}"
        d = defer.maybeDeferred(connection.ConnectionPool, uri=uri.format(**details))
        d.addCallback(_after_connect)
        return d

    @classmethod
    def disconnect(cls):
        if cls.mongo is None:
            return

        def _after_disconnect(x):
            cls.mongo = None 

        # Returns a deferred which (hopefully) fires when all
        # connections are severed
        closer = cls.mongo.disconnect()
        closer.addBoth(_after_disconnect)
        return closer

    def __eq__(self, other):
        ''' Comparison between this and another object. Also returns true if
        compared to an ObjectId that matches this objects _id '''
        if other.__class__ is self.__class__ and self._id == other._id:
            return True

        return isinstance(other, ObjectId) and other == self._id

    def __ne__(self, other):
        return not self.__eq__(other)

    @classmethod
    def getCollection(cls):
        db = getattr(cls.mongo, cls.dbname)
        collection = getattr(db, cls.collection)
        return collection

    @classmethod
    @defer.inlineCallbacks
    def findOne(cls, docid, loadRefs=False):
        if docid is not None and not isinstance(docid, ObjectId):
            # Raises exception if docid is not ObjectId-able
            docid = ObjectId(docid)
        if docid is None:
            defer.returnValue(cls())
        collection = cls.getCollection()
        doc = yield collection.find_one({'_id': docid})
        if not doc:
            err = '{} with the id {} not found'.format(cls.__name__, docid)
            raise KeyError(err)

        if "_unmarshal_class" in doc:
            newcls = cls._find_class(doc["_unmarshal_class"])
        else:
            newcls = cls
        new_object = newcls()
        new_object.setValues(doc)
        new_object.loaded = True
        if loadRefs:
            yield new_object.loadRefs()
        defer.returnValue(new_object)

    @defer.inlineCallbacks
    def load(self, docid=None):
        # mongo = yield txmongo.MongoConnectionPool('127.0.0.1', 27017)

        if docid is not None and not isinstance(docid, ObjectId):
            # Raises exception if docid is not ObjectId-able
            docid = ObjectId(docid)
        if docid is None:
            defer.returnValue(self)

        collection = self.getCollection()

        docs = yield collection.find({'_id': docid}, limit=1)
        if not len(docs):
            raise KeyError('Object id: %s not found' % docid)

        self.setValues(docs[0])

        self.loaded = True

        defer.returnValue(self)

    @classmethod
    def find(cls, search, **kwargs):
        ''' Get a list of all objects in this collection that match _search_'''
        return MongoSet(search, cls, **kwargs)._runQuery()

    @classmethod
    def find_and_modify(cls, query=None, update=None, sort=None, **kwargs):
        ''' Get a single document from `query` and modify it with `update` '''
        if query is None:
            query = {}
        assert isinstance(query, dict)
        if "new" not in kwargs:
            # Default to returning updated document
            kwargs["new"] = True
        if sort:
            if isinstance(sort, tuple):
                if not isinstance(sort[0], tuple):
                    sort = (sort, )
                sort = OrderedDict(sort)
            kwargs["sort"] = sort
        collection = cls.getCollection()
        d = collection.find_and_modify(query=query, update=update, **kwargs)

        def _after(res):
            if res is None:
                return res
            new_object = cls()
            new_object.setValues(res)
            new_object.loaded = True
            return new_object

        d.addCallback(_after)
        return d

    @classmethod
    def count(cls, search):
        collection = cls.getCollection()
        d = collection.count(search)

        def _afterCount(res):
            # count returns a float by default. Cast to int.
            return int(res)
        d.addCallback(_afterCount)
        return d

    @defer.inlineCallbacks
    def save(self):
        collection = self.getCollection()

        data = self.getValues()
        if '_id' in data and data['_id'] is None:
            del data['_id']

        if '_id' not in data:
            olddata = data.copy()
            data = self.create(data)
            newkeys = filter(lambda k: data[k] != olddata[k], data.keys())
            for i in newkeys:
                setattr(self, i, data[i])
            data['cdate'] = datetime.today()
            self.cdate = data['cdate']
        else:
            data_out = {}
            for i in self._prop_dirty:
                if i not in data or i == '_id':
                    continue
                data_out[i] = data[i]
            if not data_out:
                self._prop_dirty.clear()
                defer.returnValue(None)

            data = {'$set': data_out}
            self._prop_dirty.clear()
            out = yield collection.update({'_id': self._id}, data, safe=True)
            defer.returnValue(out)

        result = yield collection.save(data, safe=True)
        if result.__class__ is ObjectId:
            self._id = result
            self.loaded = True

        defer.returnValue(result)

    @defer.inlineCallbacks
    def insert_unique(self, query):
        ''' Atomically insert a document. Raises `DocumentExists` if `query`
        matches any documents
        '''
        assert isinstance(query, dict)
        collection = self.getCollection()
        self.cdate = datetime.today()
        data = self.getValues()
        if "_id" in data:
            del data["_id"]
        self._prop_dirty.clear()
        insert = {"$setOnInsert": data}
        out = yield collection.update(query, insert, upsert=True, safe=True)
        if "upserted" not in out or not out["upserted"]:
            raise DocumentExists("Document already exits.")
        self._id = out["upserted"]
        self.loaded = True
        defer.returnValue(out["upserted"])

    @defer.inlineCallbacks
    def remove(self):
        ''' Delete a single object '''
        if not self.loaded:
            defer.returnValue(False)

        collection = self.getCollection()
        res = yield collection.remove({'_id': self._id})
        self._prop_data.clear()
        self._prop_dirty.clear()
        self._id = None
        self.loaded = False
        defer.returnValue(res)

    @classmethod
    def aggregate(cls, spec, **kwargs):
        collection = cls.getCollection()

        return collection.aggregate(spec, **kwargs)

    @classmethod
    def _find_class(cls, name):
        ''' Find a class to unmarshal by name '''
        classes = _all_subclasses(MongoObj)
        out = filter(lambda k: k.__name__ == name, classes)
        if not out:
            # Shrug and give up?
            return cls
        return out[0]


class MongoSet(object):

    _limit = 0
    _skip = 0
    _sort = None
    _data = None
    _queryRun = False
    _result = None
    _display_timezone = None
    _use_cursor = False

    def __init__(self, search, cls, limit=0, skip=0, sort=None,
                 loadRefs=False, display_timezone=None, use_cursor=False):
        self._search = search
        self._class = cls
        self._limit = limit
        self._skip = skip
        self._sort = sort
        self._loadRefs = loadRefs
        self._display_timezone = display_timezone
        self._result = []
        self._use_cursor = use_cursor
        self._cursor = None

    def limit(self, num):
        self._limit = num

    def skip(self, num):
        self._skip = num

    def sort(self, obj):
        self._sort = obj

    def __iter__(self):
        for i in self._result:
            # yield self._applyItem(i)
            yield i

    def __len__(self):
        return len(self._result)

    def _afterQuery(self, objs):
        self._result = objs
        return self

    def hasMore(self):
        if self._cursor:
            return self._runQuery()
        else:
            return False

    @defer.inlineCallbacks
    def _runQuery(self):
        if self._cursor:
            docs, self._cursor = yield self._cursor
        else:
            collection = self._class.getCollection()
            if self._sort is not None:
                ftr = txmongo.filter.sort(self._sort)
            else:
                ftr = None
            if self._use_cursor:
                docs, self._cursor = yield collection.find(spec=self._search,
                                                           limit=self._limit,
                                                           skip=self._skip,
                                                           filter=ftr,
                                                           cursor=self._use_cursor)
            else:
                docs = yield collection.find(spec=self._search,
                                             limit=self._limit,
                                             skip=self._skip,
                                             filter=ftr,
                                             cursor=self._use_cursor)

        out = []
        for i in docs:
            o = self._applyItem(i)
            if self._loadRefs:
                yield o.loadRefs()
            out.append(o)
        # print "SENDING"
        # defer.returnValue(out)
        self._result = out
        defer.returnValue(self)

        # load refs (if there are any)
        # if self._loadRefs:
        #     for i in docs:
        #         yield i.loadRefs()
        #     schema = self._class().schema
        #     for k, v in schema.iteritems():
        #         if not isinstance(v, referenceProperty):
        #             continue
        #         ids = set()
        #         for i in docs:
        #             val = i.get(k, None)
        #             if val is None:
        #                 continue
        #             ids.add(val)
        #         if not len(ids):
        #             continue
        #         for j in chunks(list(ids), 100):
        #             tmp = yield v._refCls.find({'_id': {'$in': j}})
        #             _tmp = {}
        #             for i in tmp:
        #                 _tmp[i._id] = i
        #             del tmp
        #             for i in docs:
        #                 key = i.get(k, None)
        #                 if key in _tmp:
        #                     i[k] = _tmp[key]
        #                 else:
        #                     i[k] = None

        # self._result = docs
        # defer.returnValue(self)

    def __getitem__(self, index):
        if index.__class__ is not int:
            raise TypeError
        return self._result[index]

    def _applyItem(self, obj):
        if "_unmarshal_class" in obj and obj["_unmarshal_class"] != self._class.__name__:
            cls = self._class._find_class(obj["_unmarshal_class"])
        else:
            cls = self._class
        out = cls()
        out.display_timezone = self._display_timezone
        out.setValues(obj)
        out.loaded = True
        return out


def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    From StackOverflow: http://stackoverflow.com/a/312464/999844
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]
