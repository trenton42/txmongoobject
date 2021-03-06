from txmongoobject import model
from twisted.trial import unittest
from twisted.internet import defer
from datetime import datetime
try:
    from txmongo._pymongo.objectid import ObjectId
except ImportError:
    from bson.objectid import ObjectId
import pytz

model.MongoObj.dbname = 'test_database'


class Fragment(model.MongoObj):
    testValue = model.stringProperty()


class CollectionObject(model.MongoObj):
    testString = model.stringProperty()
    testMaxLengthString = model.stringProperty(maxLength=5)
    testInt = model.intProperty()
    testFloat = model.floatProperty()
    testBool = model.boolProperty()
    testDefaultFalse = model.boolProperty(default=False)
    testDefaultTrue = model.boolProperty(default=True)
    testDate = model.dateProperty()
    testRef = model.referenceProperty(Fragment)
    testRefList = model.listProperty(wrapper=model.referenceProperty(Fragment))
    testExtra = model.stringProperty()
    testDict = model.dictProperty(allowNone=False)
    testNoneDict = model.dictProperty(allowNone=True)

    def create(self, data):
        data['testExtra'] = 'teststring'
        return data


class KeyTestCollection(model.MongoObj):
    _testString = model.stringProperty(key='testString')
    _testInt = model.intProperty(key='testInt')
    _testFloat = model.floatProperty(key='testFloat')
    _testBool = model.boolProperty(key='testBool')
    _testDate = model.dateProperty(key='testDate')
    _testRef = model.referenceProperty(Fragment, key='testRef')


class CountCollectionObject(model.MongoObj):
    number = model.intProperty()


class DifferentCollectionObject(model.MongoObj):
    collection = "other_collection"
    data = model.intProperty(default=5)


class ChildCountObject(CountCollectionObject):
    collection = CountCollectionObject.collection


class SecondChildObject(ChildCountObject):
    pass


class TestCollection(unittest.TestCase):

    timeout = 15

    @defer.inlineCallbacks
    def setUp(self):
        # model.MongoObj.mongo = yield model.txmongo.MongoConnection('127.0.0.1', 27017)
        yield model.MongoObj.connect('127.0.0.1', 27017)

    @defer.inlineCallbacks
    def tearDown(self):
        yield model.MongoObj.disconnect()

    def test_default(self):
        col = CollectionObject()
        self.assertIdentical(col.testBool, None)
        self.assertIdentical(col.testString, None)
        self.assertIdentical(col.testInt, None)
        self.assertIdentical(col.testFloat, None)
        self.assertIdentical(col.testDate, None)
        self.assertTrue(col.testDefaultTrue, True)
        self.assertFalse(col.testDefaultFalse, False)

    @defer.inlineCallbacks
    def test_reference(self):
        col = CollectionObject()
        frag = Fragment()
        frag.testValue = 'This is a test'

        yield frag.save()

        col.testRef = frag

        yield col.save()

        self.assertEqual(col.testRef, frag)

        newobj = CollectionObject()
        yield newobj.load(col._id)

        self.assertEqual(newobj.testRef, frag)

        lrobj = yield CollectionObject.findOne(col._id)
        self.assertIsInstance(lrobj.testRef, ObjectId)

        lrobj = yield CollectionObject.findOne(col._id, loadRefs=True)
        self.assertIsInstance(lrobj.testRef, Fragment)

    @defer.inlineCallbacks
    def test_load(self):
        col = CollectionObject()
        yield col.save()

        self.assertTrue(col.loaded)

        frag = Fragment()
        yield frag.save()

        newcol = yield CollectionObject.findOne(col._id)

        self.assertTrue(newcol.loaded)

        self.assertEqual(col, newcol)

    @defer.inlineCallbacks
    def test_comparison(self):
        col = CollectionObject()
        yield col.save()

        dup = CollectionObject()
        yield dup.load(col._id)

        other = CollectionObject()
        yield other.save()

        self.assertNotEqual(col, other)
        self.assertEqual(col, dup)
        self.assertEqual(col, dup._id)

    @defer.inlineCallbacks
    def test_remove(self):
        obj = CollectionObject()
        obj.testString = 'this is a test'
        yield obj.save()

        self.assertTrue(obj.loaded)

        _id = obj._id
        yield obj.remove()

        self.assertFalse(obj.loaded, 'Object should not be loaded after being deleted')

        tmp = yield CollectionObject.find({'_id': _id})

        self.assertEqual(len(tmp), 0)

    @defer.inlineCallbacks
    def test_dirty(self):
        col = CollectionObject()
        col.testBool = False
        col.testString = 'Some string'
        col.testFloat = 1.1
        col.testInt = 5
        col.testDate = datetime.today()
        self.assertFalse(col.loaded, 'Ensure this object has not been loaded')
        self.assertIdentical(col._id, None, 'A new object must not have an _id set')
        yield col.save()

        self.assertIsInstance(col._id, ObjectId, 'Once an object is saved, _id must be an ObjectId')

        col.testFloat = 4.0

        self.assertIn('testFloat', col._prop_dirty)

        yield col.save()

        # Reload object from database

        newobj = CollectionObject()
        yield newobj.load(col._id)

        self.assertTrue(newobj.loaded)
        self.assertEqual(newobj._id, col._id)

        newobj.testBool = True

        for i in ('testString', 'testDate', 'testFloat', 'testRef', 'testInt', 'testRefList'):
            # Set all attributes to the same value. Should not set the dirty flag
            setattr(newobj, i, getattr(newobj, i))

        self.assertIn('testBool', newobj._prop_dirty)
        self.assertNotIn('testString', newobj._prop_dirty)
        self.assertNotIn('testDate', newobj._prop_dirty)
        self.assertNotIn('testFloat', newobj._prop_dirty)
        self.assertNotIn('testInt', newobj._prop_dirty)
        self.assertNotIn('testRef', newobj._prop_dirty)
        self.assertNotIn('testRefList', newobj._prop_dirty)

        yield newobj.save()

        newobj.testString = 'well, that is some new string'

        self.assertIn('testString', newobj._prop_dirty)

        yield newobj.save()

        self.assertEqual(len(newobj._prop_dirty), 0)

    def test_string(self):
        obj = CollectionObject()
        obj.testMaxLengthString = '*' * 5
        self.assertEqual(len(obj.testMaxLengthString), 5)

    @defer.inlineCallbacks
    def test_keys(self):
        ''' Ensure that keys can be changed '''
        obj = KeyTestCollection()
        obj._testString = 'sample'
        obj._testInt = 5
        obj._testFloat = 5.5
        obj._testBool = False
        # NB: Mongo ISODate()'s precision is 100ms
        sampleDate = datetime.today().replace(microsecond=0)
        obj._testDate = sampleDate
        frag = Fragment()
        yield frag.save()
        obj._testRef = frag

        yield obj.save()

        self.assertIn('testRef', obj._prop_data)
        self.assertIn('testString', obj._prop_data)
        self.assertIn('testDate', obj._prop_data)
        self.assertIn('testBool', obj._prop_data)
        self.assertIn('testFloat', obj._prop_data)
        self.assertIn('testInt', obj._prop_data)

        newobj = yield KeyTestCollection.findOne(obj._id)

        self.assertIn('testRef', newobj._prop_data)
        self.assertIn('testString', newobj._prop_data)
        self.assertIn('testDate', newobj._prop_data)
        self.assertIn('testBool', newobj._prop_data)
        self.assertIn('testFloat', newobj._prop_data)
        self.assertIn('testInt', newobj._prop_data)
        self.assertEqual(newobj._testString, 'sample')
        self.assertEqual(newobj._testInt, 5)
        self.assertEqual(newobj._testFloat, 5.5)
        self.assertIdentical(newobj._testBool, False)
        self.assertEqual(newobj._testDate, sampleDate)

        yield obj.remove()
        yield frag.remove()
        yield newobj.remove()

    @defer.inlineCallbacks
    def test_dates(self):
        ''' Ensure date processing / localization works '''
        today = datetime.today().replace(microsecond=0)
        obj = CollectionObject()
        obj.testDate = today

        yield obj.save()

        newobj = yield CollectionObject.findOne(obj._id)

        self.assertEqual(newobj.testDate, today)
        self.assertIdentical(newobj.testDate.tzinfo, None)

        newobj.display_timezone = pytz.timezone('US/Eastern')

        self.assertNotEqual(newobj.testDate.tzinfo, None)

        yield newobj.save()

        obj = yield CollectionObject.findOne(obj._id)
        self.assertEqual(obj.testDate, today)

    @defer.inlineCallbacks
    def test_count(self):
        ''' Ensure count class method works '''
        count = yield CountCollectionObject.count({})
        obj = CountCollectionObject()
        obj.number = 7
        yield obj.save()
        new_count = yield CountCollectionObject.count({})
        self.assertEqual(count, new_count - 1)
        count = yield CountCollectionObject.count({'number': 6})
        self.assertEqual(count, 0)
        self.assertIsInstance(count, int)

    @defer.inlineCallbacks
    def test_create(self):
        ''' Ensure that data set by the create method gets set '''
        p = CollectionObject()
        self.assertIdentical(p.testExtra, None)
        yield p.save()
        self.assertEqual(p.testExtra, 'teststring')
        yield p.remove()

    @defer.inlineCallbacks
    def test_insert_unique(self):
        ''' Ensure that insert_unique only inserts once for a given query '''
        p = CollectionObject()
        collection = p.getCollection()
        yield collection.remove({"testString": "bob"})
        yield collection.remove({"testString": "frank"})
        p.testString = "bob"
        out = yield p.insert_unique({"testString": "bob"})
        self.assertIsInstance(out, ObjectId)
        self.assertIsInstance(p._id, ObjectId)
        self.assertEqual(p._id, out)
        self.assertIsInstance(p.cdate, datetime)

        p2 = CollectionObject()
        p2.testString = "frank"
        out = yield p2.insert_unique({"testString": "frank"})
        self.assertIsInstance(out, ObjectId)

        f = p.insert_unique({"testString": "bob"})
        yield self.assertFailure(f, model.DocumentExists)
        f = p2.insert_unique({"testString": "frank"})
        yield self.assertFailure(f, model.DocumentExists)

    @defer.inlineCallbacks
    def test_find_and_modify(self):
        ''' Ensure that find_and_modify returns one Object, etc '''
        p = CollectionObject()
        yield p.getCollection().remove({"testInt": {"$ne": None}})
        p.testInt = 1
        yield p.save()
        out = yield p.find_and_modify({"testInt": 2}, {"$set": {"testInt": 2}})
        self.assertIsNone(out)
        out = yield p.find_and_modify({"testInt": 1}, {"$set": {"testInt": 2}})
        self.assertEqual(out.testInt, 2)
        self.assertEqual(out._id, p._id)
        yield p.getCollection().remove({})
        for i in range(10):
            p.getCollection().insert({"testInt": 1})
        cnt = 0
        search = {"testInt": 1}
        update = {"$set": {"testInt": 2}}
        nxt = yield CollectionObject.find_and_modify(search, update)
        while nxt:
            cnt += 1
            nxt = yield CollectionObject.find_and_modify(search, update)
        self.assertEqual(cnt, 10)
        yield p.getCollection().remove({"testInt": {"$ne": None}})
        chars = ["e", "d", "c", "b", "a"]

        for k, v in enumerate(chars):
            tmp = CollectionObject()
            tmp.testInt = k
            tmp.testString = v
            yield tmp.save()
        search = {
            "testInt": {
                "$ne": None
            }
        }
        update = {
            "$set": {
                "testFloat": 7
            }
        }
        nxt = yield CollectionObject.find_and_modify(query=search, update=update, sort={"testInt": 1})
        self.assertEqual(nxt.testInt, 0, "Sort Ascending")
        nxt = yield CollectionObject.find_and_modify(query=search, update=update, sort={"testInt": -1})
        self.assertEqual(nxt.testInt, 4, "Sort Descending")

    @defer.inlineCallbacks
    def test_specified_collection(self):
        obj = DifferentCollectionObject()
        obj.data = 7
        yield obj.save()
        collection = obj.getCollection()
        self.assertEqual(str(collection).split('.')[-1], "other_collection")
        collection = CountCollectionObject.getCollection()
        yield collection.remove({})
        child = ChildCountObject()
        child.number = 99959
        yield child.save()
        self.assertEqual(child._unmarshal_class, 'ChildCountObject')

        results = yield CountCollectionObject.find({"number": 99959})
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], ChildCountObject)

        second = SecondChildObject()
        second.number = 99969
        yield second.save()
        self.assertEqual(second._unmarshal_class, 'SecondChildObject')
        results = yield CountCollectionObject.find({"number": 99969})
        self.assertEqual(len(results), 1)
        self.assertIsInstance(results[0], SecondChildObject)

        findoneres = yield CountCollectionObject.findOne(child._id)
        self.assertIsInstance(findoneres, ChildCountObject)

    @defer.inlineCallbacks
    def test_cursor(self):
        for i in range(10000):
            p = CollectionObject()
            p.testInt = i
            p.testString = "test_cursor"
            yield p.save()

        count = yield CollectionObject.count({"testString": "test_cursor"})

        res = yield CollectionObject.find({"testString": "test_cursor"}, use_cursor=True, sort=[["testInt", 1]])
        ccount = 0
        more = res._cursor
        while more:
            ccount += len(res)
            more = yield res.hasMore()
        yield CollectionObject.getCollection().remove({"testString": "test_cursor"})

        self.assertEqual(count, ccount)

    def test_dictProperty(self):
        a = CollectionObject()
        self.assertEqual(a.testDict, {})
        a.testDict["Test"] = True

        b = CollectionObject()
        self.assertEqual(b.testDict, {})
