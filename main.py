import json
import time

from flask import Flask, jsonify, request
from flask import Response
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from pymongo import MongoClient, ASCENDING, DESCENDING
from types import SimpleNamespace

app = Flask(__name__)
CORS(app)

limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["50 per minute"],
    storage_uri="mongodb://localhost:27017",
)

client = MongoClient('mongodb://127.0.0.1:27017')
db = client['SSE']


@app.route('/send', methods=["POST"])
def send():
    _body = json.loads(request.data.decode())
    print(_body)
    channel_collection = db['im_' + _body.get('channelId')]

    def get_incremented_id():
        if channel_collection.count_documents({}) != 0:
            for doc in channel_collection.find().sort('msg_seq', DESCENDING).limit(1):
                return doc['msg_seq'] + 1
        else:
            return 1

    def insert_document_with_auto_incremented_id(document):
        new_id = get_incremented_id()
        document['msg_seq'] = new_id
        channel_collection.insert_one(document)

    insert_document_with_auto_incremented_id(_body)
    return jsonify({
        'success': True
    })


@app.route('/stream')
def stream():
    channel_id = request.args.get('channelId')
    print('watch channel id:' + channel_id)
    channel_collection = db['im_' + channel_id]

    def event_stream():
        _msg_seq = 0
        while True:

            if _msg_seq == 0:  # 返回历史消息
                for doc in channel_collection.find().sort('msg_seq', ASCENDING).limit(10):
                    _obj = SimpleNamespace(**doc)
                    if _msg_seq < _obj.msg_seq:
                        _msg_seq = _obj.msg_seq
                    yield 'id: {}\ndata: {}\n\n'.format(_obj.msg_seq,
                                                        json.dumps(
                                                            {"from_user_id": _obj.from_user_id, "msg": _obj.msg,
                                                             "msg_seq": _obj.msg_seq}))

            else:  # 返回下一条序列消息
                for doc in channel_collection.find({'msg_seq': _msg_seq + 1}):
                    _obj = SimpleNamespace(**doc)
                    _msg_seq += 1
                    yield 'id: {}\ndata: {}\n\n'.format(_obj.msg_seq,
                                                        json.dumps(
                                                            {"from_user_id": _obj.from_user_id, "msg": _obj.msg,
                                                             "msg_seq": _obj.msg_seq}))
            time.sleep(0.8)

    return Response(event_stream(), mimetype="text/event-stream",
                    headers={'Cache-Control': 'no-cache', 'Connection': 'keep-alive', 'X-Accel-Buffering': 'no'})


if __name__ == '__main__':
    app.run(host="0.0.0.0")
