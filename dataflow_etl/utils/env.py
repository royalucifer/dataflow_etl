# -*- coding: UTF-8 -*-
from avro.schema import parse as Parse

CHANNEL_LISTS = ["education", "local", "real_time", "china", "society", "digital", "travel", "entertainment",
                 "health", "car", "game", "house", "fun", "fashion", "politic", "news", "shopping", "life",
                 "finance", "investment", "global", "topic", "comment", "sport", "book", "e_commerce", "other"]

# CHANNEL_LISTS = ["文教", "地方", "即時", "兩岸", "社會", "數位", "旅遊", "娛樂", "健康",
#                  "汽車", "遊戲", "房產", "趣聞", "時尚", "政治", "要聞", "消費", "生活",
#                  "財經", "投資", "國際", "專題", "評論", "運動", "閱讀", "電商", "其他"]

PROJECT_FIELDS = {
    "ALL": ["totalSessions", "totalDuration", "totalPageviews", "avgTimePerSession", "avgPageviewsPerSession"],
    "CH": ["cookies", "channel", "totalPageviews"],
    "USER": ["cookies", "visitWeek", "visitTime", "device", "brand", "region", "source", "num"]
}

COLUMNS = {
    "VIEW": ["cookies", "book", "car", "china", "comment", "digital", "e_commerce", "education", "entertainment",
             "fashion", "finance", "fun", "game", "global", "health", "house", "investment", "life", "local",
             "news", "other", "politic", "real_time", "shopping", "society", "sport", "topic", "travel",
             "totalSessions", "totalPageviews", "avgTimePerSession", "avgPageviewsPerSession"],
    "USER": ["cookies", "visitWeek", "visitTime", "device", "brand", "region", "source", "num"]
}

USER_SCHEMA = Parse('''
  {"type": "record",
   "name": "User",
   "fields": [
       {"name": "cookies", "type": "string"},
       {"name": "visitWeek",  "type": ["string", "null"]},
       {"name": "visitTime", "type": ["string", "null"]},
       {"name": "device", "type": ["string", "null"]},
       {"name": "brand", "type": ["string", "null"]},
       {"name": "region", "type": ["string", "null"]},
       {"name": "source", "type": ["string", "null"]},
       {"name": "num", "type": "int"}
   ]
  }
''')

VIEW_SCHEMA = Parse('''
  {"type": "record",
   "name": "View",
   "fields": [
       {"name": "cookies", "type": "string"},
       {"name": "book",  "type": ["int", "null"]},
       {"name": "car", "type": ["int", "null"]},
       {"name": "china", "type": ["int", "null"]},
       {"name": "comment", "type": ["int", "null"]},
       {"name": "digital", "type": ["int", "null"]},
       {"name": "e_commerce", "type": ["int", "null"]},
       {"name": "education", "type": ["int", "null"]},
       {"name": "entertainment", "type": ["int", "null"]},
       {"name": "fashion", "type": ["int", "null"]},
       {"name": "finance", "type": ["int", "null"]},
       {"name": "fun", "type": ["int", "null"]},
       {"name": "game", "type": ["int", "null"]},
       {"name": "global", "type": ["int", "null"]},
       {"name": "health", "type": ["int", "null"]},
       {"name": "house", "type": ["int", "null"]},
       {"name": "investment", "type": ["int", "null"]},
       {"name": "life", "type": ["int", "null"]},
       {"name": "local", "type": ["int", "null"]},
       {"name": "news", "type": ["int", "null"]},
       {"name": "other", "type": ["int", "null"]},
       {"name": "politic", "type": ["int", "null"]},
       {"name": "real_time", "type": ["int", "null"]},
       {"name": "shopping", "type": ["int", "null"]},
       {"name": "society", "type": ["int", "null"]},
       {"name": "sport", "type": ["int", "null"]},
       {"name": "topic", "type": ["int", "null"]},
       {"name": "travel", "type": ["int", "null"]},
       {"name": "totalSessions", "type": ["int", "null"]},
       {"name": "totalPageviews", "type": ["int", "null"]},
       {"name": "avgTimePerSession", "type": ["float", "null"]},
       {"name": "avgPageviewsPerSession", "type": ["float", "null"]}
   ]
  }
''')