from peewee import *

db = MySQLDatabase(database='test',
                   host='localhost',
                   user='root',
                   password='root')
class BaseModel(Model):
    class Meta:
        database = db
        # primary_key = CompositeKey('ac_num', 'take_off_datetime')

class User(BaseModel):
    username = TextField()

class Tweet(BaseModel):
    user = ForeignKeyField(User, backref='tweets')
    content = TextField()

db.create_tables([User, Tweet])

u1 = User.create(username = 'wm')
Tweet.create(user = u1, content = 'haha')
Tweet.create(user = u1, content = 'tutu')

query = Tweet.select().where(Tweet.user == u1)

# print(query[0].content)
# print(query[1].content)
for t in u1.tweets:
    print(t.content)