from collections import deque
from itertools import chain
from typing import Iterable

from graph import Edge, Vertex
from tree import RegionData



class ItemData:
    def __init__(self, id: int):
        self.parent = id
        self.rank: int = 0

class FindAndUnionHelper:
    def __init__(self):
        self.item_data = dict[int, ItemData]()

    def add(self, vertices: Iterable[Vertex]) -> None:
        for vert in vertices:
            self.item_data[vert.id] = ItemData(vert.id)

    def find(self, vert_id: int) -> int:
        y = self.item_data[vert_id].parent
        if vert_id == y:
            return vert_id

        z = self.find(y)
        self.item_data[vert_id].parent = z
        return z

    def union(self, vert1_id: int, vert2_id: int) -> bool:
        root1 = self.find(vert1_id)
        root2 = self.find(vert2_id)
        if root1 == root2:
            return False

        root1Rank = self.item_data[root1].rank
        root2Rank = self.item_data[root2].rank
        if root1Rank > root2Rank:
            self.item_data[root2].parent = root1
        else:
            self.item_data[root1].parent = root2
            if root1Rank == root2Rank:
                self.item_data[root2].rank += 1
        return True


class ConsolidatorHelper:
    def __init__(self, regions: Iterable[RegionData]):
        self.item_data = dict[int, ItemData]()
        self.region_ids = dict[int, int]()
        self.next_region_id = 0
        self.detached_verts = set[Vertex]()
        vert: Vertex
        for vertices in regions:
            region_id = self.get_new_region_id()
            for vert in vertices:
                self.item_data[vert.id] = ItemData(vert.id)
                self.region_ids[vert.id] = region_id

    def get_vertex_region_id(self, vert_id: int) -> int:
        return self.region_ids[vert_id]

    def get_new_region_id(self) -> int:
        id = self.next_region_id
        self.next_region_id += 1
        return id

    def get_detached_verts(self) -> set[Vertex]:
        return self.detached_verts

    def add_detached_verts(self, vertices: Iterable[Vertex]) -> None:
        for vert in vertices:
            self.detached_verts.add(vert)

    def set_vertex_region(self, vert_id: int, region_id: int) -> None:
        self.region_ids[vert_id] = region_id

    def get_regions(self, vertices: Iterable[Vertex]) -> list[list[Vertex]]:
        main_list = list[list[Vertex]]()
        for i in range(self.next_region_id):
            main_list.append(list[Vertex]())
        
        for vert in vertices:
            region_id = self.get_vertex_region_id(vert.id)
            if region_id == -1:
                raise ValueError()
            main_list[region_id].append(vert)
        return main_list


def consolidate_regions(regions : Iterable[list[Vertex]]) -> list[list[Vertex]]:
    cons_helper = ConsolidatorHelper(regions)
    detached_helper = FindAndUnionHelper()

    for vertices in regions:
        regionHelper = FindAndUnionHelper()
        regionHelper.add(vertices)
        find_union_sets(vertices, cons_helper, regionHelper)

        grouped_verts = sorted(group_vertices(vertices, regionHelper), key=len, reverse=True)
        for i in range(1, len(grouped_verts)):
            detached_helper.add(grouped_verts[i])
            cons_helper.add_detached_verts(grouped_verts[i])
            for vert in grouped_verts[i]:
                cons_helper.set_vertex_region(vert.id, -1)

    detached_verts = cons_helper.get_detached_verts()
    find_detached_sets(detached_verts, detached_helper)
    detached_groups = group_vertices(detached_verts, detached_helper)
    for group in detached_groups:
        move_detached_nodes(group, cons_helper)

    return cons_helper.get_regions(chain.from_iterable(regions))


def find_union_sets(vertices: list[Vertex], consHelper: ConsolidatorHelper, regionHelper: FindAndUnionHelper) -> None:
    vert: Vertex
    edge: Edge
    cur_region_id = consHelper.get_vertex_region_id(vertices[0].id)

    for vert in vertices:
        for edge in vert.get_edges():
            other_vert = edge.get_other_vert(vert)
            if cur_region_id != consHelper.get_vertex_region_id(other_vert.id):
                continue
            regionHelper.union(vert.id, other_vert.id)


def find_detached_sets(vertices: set[Vertex], detached_helper: FindAndUnionHelper) -> None:
    for vert in vertices:
        for edge in vert.get_edges():
            other_vert = edge.get_other_vert(vert)
            if not other_vert in vertices:
                continue
            detached_helper.union(vert.id, other_vert.id)


def group_vertices(vertices: Iterable[Vertex], unionHelper: FindAndUnionHelper) -> Iterable[list[Vertex]]:
    vert_by_set = dict[list[Vertex]]()
    vert: Vertex
    for vert in vertices:
        root = unionHelper.find(vert.id)
        if root not in vert_by_set:
            l = list[Vertex]()
            vert_by_set[root] = l
        else:
            l = vert_by_set[root]
        l.append(vert)
    return vert_by_set.values()


def move_detached_nodes(vertices: list[Vertex], helper: ConsolidatorHelper) -> None:
    target_region_id = find_target_parent_id(vertices[0], helper)
    for vert in vertices:
        helper.set_vertex_region(vert.id, target_region_id)


def find_target_parent_id(vert: Vertex, helper: ConsolidatorHelper) -> int:
    cur_region_id = helper.get_vertex_region_id(vert.id)

    visited = set[int]()
    queue = deque[Vertex]()
    queue.append(vert)

    while any(queue):
        vert = queue.popleft()
        if vert.id in visited:
            continue
        visited.add(vert.id)

        for edge in vert.get_edges():
            other_vert = edge.get_other_vert(vert)
            target_region_id = helper.get_vertex_region_id(other_vert.id)
            if cur_region_id != target_region_id:
                return target_region_id

        for edge in vert.get_edges():
            other_vert = edge.get_other_vert(vert)
            if other_vert.id in visited:
                continue
            queue.append(other_vert)
    raise ValueError()
