import json, os, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import urllib.parse

import redis



class NodeId:
    def __init__(self, id: int, region_id: int):
        self.id = id
        self.region_id = region_id

    def to_list(self) -> list:
        return [ self.id, self.region_id ]

    def from_dict(data: dict) -> 'NodeId':
        return NodeId(data['id'], data['region_id'])

    def from_list(data: list) -> 'NodeId':
        return NodeId(data[0], data[1])

class NodeBody:
    def __init__(self, id: NodeId, cord_x: int, cord_y: int):
        self.id = id
        self.cord_x = cord_x
        self.cord_y = cord_y
    
    def to_dict(self) -> dict:
        return dict(id=self.id.id, region_id=self.id.region_id, cord_x=self.cord_x, cord_y=self.cord_y)

    def from_dict(data: dict) -> 'NodeBody':
        return NodeBody(NodeId.from_dict(data), data['cord_x'], data['cord_y'])

class RequestBody:
    def __init__(self, request_id: int, source: NodeId, target: NodeId, path: list[NodeBody], cost: int):
        self.request_id = request_id
        self.source = source
        self.target = target
        self.path = path
        self.cost = cost
    
    def to_dict(self) -> dict:
        source = self.source.to_list()
        target = self.target.to_list()
        path_list = [ n.to_dict() for n in self.path ]
        return dict(request_id=self.request_id, source=source, target=target, path=path_list, cost=self.cost)

    def from_json(text: str) -> 'RequestBody':
        obj = json.loads(text)
        path = [ NodeBody.from_dict(d) for d in obj['path'] ]
        return RequestBody(obj['request_id'], NodeId.from_list(obj['source']), NodeId.from_list(obj['target']), path, obj['cost'])

class ResponseNode:
    def __init__(self, id: int, x: int, y: int):
        self.id = id
        self.x = x
        self.y = y

    def to_dict(self) -> dict:
        return dict(id=self.id, x=self.x, y=self.y)

class ResponseBody:
    def __init__(self, request: RequestBody):
        self.src = request.source.id
        self.dst = request.target.id
        self.path = [ ResponseNode(n.id.id, n.cord_x, n.cord_y) for n in request.path ]
        self.cost = request.cost

    def to_dict(self) -> dict:
        path = [ n.to_dict() for n in self.path ]
        return dict(src=self.src, dst=self.dst, path=path, cost=self.cost)

class RequestCounter:
    def __init__(self):
        self.count = 0
        self.lock = threading.Lock()

    def next(self) -> int:
        with self.lock:
            self.count += 1
            return self.count


address = sys.argv[1]
port = int(sys.argv[2])

param_from_args = len(sys.argv) > 3
if param_from_args:
    redis_address = sys.argv[3]
    redis_port = sys.argv[4]
else:
    redis_address = os.environ['REDIS_SERVICE_HOST']
    redis_port = os.environ['REDIS_SERVICE_PORT']

redis_handle = redis.Redis(host=redis_address, port=redis_port)
request_counter = RequestCounter()

class GraphFinderHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        url = urllib.parse.urlparse(self.path)
        if url.path != '/':
            self.send_response(404)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            return

        params = urllib.parse.parse_qs(url.query)
        if 'src' not in params or 'dst' not in params:
            self.send_response(400)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            return
        
        src_id = int(params['src'][0])
        dst_id = int(params['dst'][0])
        src_region = int(redis_handle.get(f'node_region_{src_id}'))
        dst_region = int(redis_handle.get(f'node_region_{dst_id}'))

        request_id = request_counter.next()
        srcnid = NodeId(src_id, src_region)
        dstnid = NodeId(dst_id, dst_region)

        request = RequestBody(request_id, srcnid, dstnid, [], 0)
        request_json = json.dumps(request.to_dict())

        ch = redis_handle.pubsub()
        ch.subscribe([ f'results_{request_id}' ])

        redis_handle.publish(f'node_{src_id}', request_json)

        while True:
            msg = ch.get_message(True, timeout=0.1)
            if msg is None:
                continue
            response = ResponseBody(RequestBody.from_json(msg['data']))
            ch.unsubscribe()
            break

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        response_json = json.dumps(response.to_dict())
        self.wfile.write(bytes(response_json, 'utf-8'))


server = ThreadingHTTPServer((address, port), GraphFinderHandler)
print(f'Server running on {address}:{port}')
print(f'Redis on {redis_address}:{redis_port}')

try:
    server.serve_forever()
except KeyboardInterrupt:
    pass

server.server_close()
print('Server stopped')
