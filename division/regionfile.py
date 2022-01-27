import os
import sys

from gparser import GraphParser
from pathlib import Path



def delete_folder(pth):
    for sub in pth.iterdir():
        if sub.is_dir():
            delete_folder(sub)
        else :
            sub.unlink()
    pth.rmdir()

vert_filename = sys.argv[1]
edge_filename = sys.argv[2]
regions_filename = sys.argv[3]

if os.path.exists('regions'):
    delete_folder(Path('regions'))
os.mkdir('regions')

with open(vert_filename, 'r') as vert_stream, open(edge_filename, 'r') as edge_stream, open(regions_filename, 'r') as region_stream:
    parser = GraphParser()
    parser.parse_csv(vert_stream, edge_stream)

    g = parser.get_graph()

    region_data = region_stream.readlines()
    region_count = len(region_data) // 2

    vert_regions = dict[int, int]()
    for i in range(region_count):
        region_id = int(region_data[i * 2])
        vert_ids = [int(vert_id) for vert_id in region_data[i * 2 + 1].split()]
        for id in vert_ids:
            vert_regions[id] = region_id

    for i in range(region_count):
        region_id = int(region_data[i * 2])
        vert_ids = [int(vert_id) for vert_id in region_data[i * 2 + 1].split()]

        saved_verts = set[int]()
        with open(os.path.join('regions', f'nodes_{region_id}.csv'), 'x') as output_stream:
            for vert_id in vert_ids:
                vert = g.get_vertex(vert_id)
                vert_line = f'{vert_id},{vert.location.x},{vert.location.y},{region_id}\n'
                output_stream.write(vert_line)
                saved_verts.add(vert_id)

            for vert_id in vert_ids:
                vert = g.get_vertex(vert_id)
                for edge in vert.get_edges():
                    other_vert = edge.get_other_vert(vert)
                    if other_vert.id not in saved_verts:
                        vert_line = f'{other_vert.id},{other_vert.location.x},{other_vert.location.y},{vert_regions[other_vert.id]}\n'
                        output_stream.write(vert_line)
                        saved_verts.add(other_vert.id)
