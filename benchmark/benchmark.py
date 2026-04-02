import asyncio
import collections
import csv
import dataclasses
import json
import math
import os
import random
import time
import uuid
from typing import Union, List, Dict, Set, Tuple, Any

FILENAMES = [
    'resultsA.csv',
    'resultsB.csv',
    'resultsC.csv',
]

FIELDNAMES = [
    'topology',
    'threads',
    'putRatio',
    'totalOps',
    'keySpace',
    'throughputOpsSec',
    'avgMs',
    'p50Ms',
    'p75Ms',
    'p95Ms',
    'p99Ms',
    'convergenceTimeMs',
]

ENCODING = 'utf-8'

HOST = '127.0.0.1'
PORT = 8081

MASTERS_COUNT, FOLLOWERS_COUNT = 4, 6
NODES_COUNT = MASTERS_COUNT + FOLLOWERS_COUNT

SOCKET_COUNT = 256


@dataclasses.dataclass
class Node:
    node_id: str
    host: str
    port: int
    role: str

    def to_dict(self):
        return {
            'node_id': self.node_id,
            'hostname': self.host,
            'port': self.port,
            'role': self.role,
        }


class ClusterAPI:
    NODES = [
        Node(
            node_id=chr(ord('A') + index),
            host=HOST,
            port=PORT + index,
            role='master' if index < MASTERS_COUNT else 'follower',
        )
        for index in range(NODES_COUNT)
    ]
    MASTERS, FOLLOWERS = NODES[:MASTERS_COUNT], NODES[MASTERS_COUNT:]

    @staticmethod
    def generate_put(request_id: str, key_space: int) -> Dict[str, Union[int, str]]:
        return {
            'type': 'CLIENT_PUT_REQUEST',
            'request_id': request_id,
            'client_id': str(uuid.uuid4()),
            'key': f'key-{random.randint(0, key_space)}',
            'value': f'value-{uuid.uuid4()}',
        }

    @staticmethod
    def generate_get(request_id: str, key_space: int) -> Dict[str, Union[int, str]]:
        return {
            'type': 'CLIENT_GET_REQUEST',
            'request_id': request_id,
            'client_id': str(uuid.uuid4()),
            'key': f'key-{random.randint(0, key_space)}',
        }

    def generate_masters_mesh(self) -> Dict[str, List[Dict[str, Union[str, int]]]]:
        result = collections.defaultdict(list)

        for from_node in self.MASTERS:
            for to_node in self.MASTERS:
                if from_node.node_id == to_node.node_id:
                    continue
                result[from_node.node_id].append(to_node.to_dict())

        return dict(result)

    def generate_masters_ring(self) -> Dict[str, List[Dict[str, Union[str, int]]]]:
        result = collections.defaultdict(list)

        masters = sorted(self.MASTERS, key=lambda node: node.node_id)
        for index, node in enumerate(masters):
            next_node = masters[(index + 1) % len(masters)]
            if node.node_id != next_node.node_id:
                result[node.node_id].append(next_node.to_dict())

        return dict(result)

    def generate_masters_star(self, center_id: str = 'A') -> Dict[str, List[Dict[str, Union[str, int]]]]:
        result = collections.defaultdict(list)

        center = None
        for node in self.MASTERS:
            if node.node_id == center_id:
                center = node
                break

        for node in self.MASTERS:
            if node.node_id == center.node_id:
                continue
            result[center.node_id].append(node.to_dict())
            result[node.node_id].append(center.to_dict())

        return dict(result)

    def generate_followers(self) -> Dict[str, List[Dict[str, Union[str, int]]]]:
        result = collections.defaultdict(list)

        for index, follower in enumerate(self.FOLLOWERS):
            master = self.MASTERS[index % len(self.MASTERS)]
            result[master.node_id].append(follower.to_dict())

        return dict(result)

    def generate_cluster_update(
            self,
            topology: str,
            min_delay_ms: int = 0,
            max_delay_ms: int = 0,
    ) -> Dict[str, Union[int, str, Dict]]:
        return {
            'type': 'CLUSTER_UPDATE_REQUEST',
            'request_id': str(uuid.uuid4()),
            'next_masters': self.__getattribute__(f'generate_masters_{topology}')(),
            'followers': self.generate_followers(),
            'nodes': {node.node_id: node.to_dict() for node in self.NODES},
            'min_delay_ms': min_delay_ms,
            'max_delay_ms': max_delay_ms,
        }


def percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return float('nan')

    if len(sorted_vals) == 1:
        return float(sorted_vals[0])

    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)

    if f == c:
        return float(sorted_vals[int(k)])

    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)

    return float(d0 + d1)


def generate_row(
        topology: str,
        threads: int,
        put_ratio: float,
        total_ops: int,
        key_space: int,
        throughput_ops_sec: float,
        avg_ms: float,
        p50_ms: float,
        p75_ms: float,
        p95_ms: float,
        p99_ms: float,
) -> Dict[str, Union[int, str, float]]:
    return {
        'topology': topology,
        'threads': threads,
        'putRatio': put_ratio,
        'totalOps': total_ops,
        'keySpace': key_space,
        'throughputOpsSec': round(throughput_ops_sec, 3),
        'avgMs': round(avg_ms, 3),
        'p50Ms': round(p50_ms, 3),
        'p75Ms': round(p75_ms, 3),
        'p95Ms': round(p95_ms, 3),
        'p99Ms': round(p99_ms, 3),
        'convergenceTimeMs': '',
    }


async def reader_loop(
        reader: asyncio.StreamReader,
        processed: Set[str],
        finish_time_by_request_id: collections.defaultdict[str, float],
) -> None:
    while True:
        line = await reader.readline()
        if not line:
            return

        response = json.loads(line.decode())
        if response['type'] in ('CLIENT_GET_RESPONSE', 'CLIENT_PUT_RESPONSE'):
            processed.add(response['request_id'])
            finish_time_by_request_id[response['request_id']] = time.perf_counter()


def encode(obj):
    if isinstance(obj, ClusterAPI.Node):
        return obj.__dict__
    raise TypeError(f'{type(obj)} is not JSON serializable')


async def send(
        writer: asyncio.StreamWriter,
        payload: Dict[str, Union[str, int]],
        start_time_by_request_id: collections.defaultdict[str, float],
) -> None:
    request_id = payload.get('request_id')
    if request_id is not None:
        start_time_by_request_id[request_id] = time.perf_counter()

    msg = json.dumps(payload, ensure_ascii=False, default=encode) + '\n'
    writer.write(msg.encode(ENCODING))
    await writer.drain()


async def open_connection(
        node_id: str,
        host: str,
        port: int,
) -> Tuple[str, Any]:
    return node_id, *await asyncio.open_connection(host, port)


async def benchmark(
        topology: str,
        put_ratio: float,
        threads: int,
        total_ops: int,
        key_space: int,
        filename: str,
) -> None:
    cluster_api = ClusterAPI()

    processed = set()
    start_time_by_request_id = collections.defaultdict(float)
    finish_time_by_request_id = collections.defaultdict(float)
    writers_by_node_id = collections.defaultdict(list)
    counter_by_node_id = collections.defaultdict(int)
    readers = []

    open_connections_tasks = []
    for index in range(SOCKET_COUNT):
        node = cluster_api.NODES[index % len(cluster_api.NODES)]
        open_connections_tasks.append(open_connection(node.node_id, node.host, node.port))

    results = await asyncio.gather(*open_connections_tasks)

    for node_id, reader, writer in results:
        readers.append(
            asyncio.create_task(
                reader_loop(
                    reader=reader,
                    processed=processed,
                    finish_time_by_request_id=finish_time_by_request_id,
                )
            )
        )
        writers_by_node_id[node_id].append(writer)

    cluster_update = cluster_api.generate_cluster_update(topology=topology)

    set_configuration_tasks = []
    for node in cluster_api.NODES:
        set_configuration_tasks.append(
            send(
                writers_by_node_id[node.node_id][0],
                cluster_update,
                start_time_by_request_id,
            )
        )
    await asyncio.gather(*set_configuration_tasks)

    requests_tasks = []
    masters = cluster_api.MASTERS

    for index in range(total_ops):
        request_id = str(uuid.uuid4())
        start_time_by_request_id[request_id] = -1.0
        finish_time_by_request_id[request_id] = -1.0

        if random.random() <= put_ratio:
            payload = cluster_api.generate_put(request_id, key_space)
            node = masters[index % len(masters)]
        else:
            payload = cluster_api.generate_get(request_id, key_space)
            node = cluster_api.NODES[index % len(cluster_api.NODES)]

        node_id = node.node_id
        writer = writers_by_node_id[node_id][counter_by_node_id[node_id]]

        requests_tasks.append(
            send(
                writer,
                payload,
                start_time_by_request_id,
            )
        )
        counter_by_node_id[node_id] = (counter_by_node_id[node_id] + 1) % len(writers_by_node_id[node_id])

    await asyncio.gather(*requests_tasks)

    while len(processed) < total_ops:
        await asyncio.sleep(0.1)

    for task in readers:
        task.cancel()

    latencies_ms = []
    starts = []
    finishes = []

    for request_id in processed:
        start_time = start_time_by_request_id[request_id]
        finish_time = finish_time_by_request_id[request_id]

        if start_time <= 0 or finish_time <= 0 or finish_time < start_time:
            continue

        starts.append(start_time)
        finishes.append(finish_time)
        latencies_ms.append((finish_time - start_time) * 1000.0)

    latencies_ms.sort()

    total_done = len(latencies_ms)
    t_start = min(starts)
    t_end = max(finishes)

    throughput_ops_sec = total_done / (t_end - t_start)
    avg_ms = sum(latencies_ms) / total_done
    p50_ms = percentile(latencies_ms, 50)
    p75_ms = percentile(latencies_ms, 75)
    p95_ms = percentile(latencies_ms, 95)
    p99_ms = percentile(latencies_ms, 99)

    need_header = (not os.path.exists(filename)) or (os.path.getsize(filename) == 0)

    with open(filename, 'a', newline='', encoding=ENCODING) as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        if need_header:
            writer.writeheader()

        writer.writerow(
            generate_row(
                topology=topology,
                threads=threads,
                put_ratio=put_ratio,
                total_ops=total_done,
                key_space=key_space,
                throughput_ops_sec=throughput_ops_sec,
                avg_ms=avg_ms,
                p50_ms=p50_ms,
                p75_ms=p75_ms,
                p95_ms=p95_ms,
                p99_ms=p99_ms,
            )
        )

    for _, _, writer in results:
        writer.close()
    await asyncio.gather(*(writer.wait_closed() for _, _, writer in results), return_exceptions=True)


async def main():
    # A
    for topology in ['mesh', 'ring', 'star']:
        for put_ratio in [0.8, 0.2]:
            await benchmark(
                topology=topology,
                put_ratio=put_ratio,
                threads=16,
                total_ops=200_000,
                key_space=10_000,
                filename=FILENAMES[0],
            )

    # B
    for key_space in [5, 10_000]:
        await benchmark(
            topology='mesh',
            put_ratio=1.0,
            threads=16,
            total_ops=100_000, # памяти не хватало
            key_space=key_space,
            filename=FILENAMES[1],
        )

    # C
    for topology in ['mesh', 'star']:
        await benchmark(
            topology=topology,
            put_ratio=0.8,
            threads=16,
            total_ops=200_000,
            key_space=10_000,
            filename=FILENAMES[2],
        )


if __name__ == '__main__':
    asyncio.run(main())
